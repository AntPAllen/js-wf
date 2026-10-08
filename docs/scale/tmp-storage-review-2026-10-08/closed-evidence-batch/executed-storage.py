import sys,os,json,subprocess,shutil,datetime,stat
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
base=repo/'docs/scale/tmp-storage-review-2026-10-08/closed-evidence-batch'
stage=Path('/tmp/js-wf-tmp-evidence-batch-staging-20261008')
archive=Path('/tmp/js-wf-tmp-evidence-batch-20261008.tar.gz')
def closure(roots):
    roots=list(roots); blocked={};limits=[];fds=0
    def check(path,detail):
        if not path.startswith('/'):return
        for root in roots:
            s=str(root)
            if path==s or path.startswith(s+'/'):blocked.setdefault(s,[]).append(detail)
    for p in Path('/proc').glob('[0-9]*'):
        for leaf in ('exe','cwd','root'):
            try:check(os.readlink(p/leaf),str(p/leaf))
            except PermissionError:limits.append(str(p/leaf))
            except (FileNotFoundError,ProcessLookupError):pass
        try:
            for arg in (p/'cmdline').read_bytes().split(b'\0'):
                for root in roots:
                    if str(root).encode() in arg:blocked.setdefault(str(root),[]).append(str(p/'cmdline'))
        except PermissionError:limits.append(str(p/'cmdline'))
        except (FileNotFoundError,ProcessLookupError):pass
        try:tasks=list((p/'task').iterdir())
        except PermissionError:limits.append(str(p/'task'));continue
        except (FileNotFoundError,ProcessLookupError):continue
        for t in tasks:
            try:entries=list((t/'fd').iterdir())
            except PermissionError:limits.append(str(t/'fd'));continue
            except (FileNotFoundError,ProcessLookupError):continue
            for fd in entries:
                try:target=os.readlink(fd);fds+=1;check(target.removesuffix(' (deleted)'),str(fd))
                except PermissionError:limits.append(str(fd))
                except (FileNotFoundError,ProcessLookupError):pass
    ids=subprocess.check_output(['docker','ps','-aq'],text=True).split()
    containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
    for c in containers:
        for m in c['Mounts']:
            source=str(Path(m['Source']).resolve());check(source,'docker:'+c['Id'])
            for r in roots:
                if str(r).startswith(source.rstrip('/')+'/'):blocked.setdefault(str(r),[]).append('docker-ancestor:'+source)
    loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
    for d in loops['loopdevices']:check(str(Path(d['back-file']).resolve()),'loop:'+d['name'])
    mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
    def walk(rows):
        for row in rows:
            check(row['target'],'mount-target');check(row['source'],'mount-source');walk(row.get('children',[]))
    walk(mounts['filesystems'])
    return dict(blocked=blocked,permission_limits=sorted(set(limits)),visible_descriptors=fds,docker_ids_including_stopped=ids,loopdevices=loops,mounts=mounts)

def capture():
    assert not stage.exists() and not archive.exists() and not base.exists()
    selected=[];skipped={}
    for root in sorted(Path('/tmp').iterdir()):
        if not root.name.startswith(('js-wf-','storage-','checkpoint4160-')) or '20261008' in root.name or root.is_symlink() or not root.is_dir():continue
        members=[root,*root.rglob('*')]
        if any(p.name=='.git' or p.is_symlink() or not (p.is_file() or p.is_dir()) for p in members):
            skipped[str(root)]='Git metadata, symlink or special file';continue
        size=sum(p.stat().st_blocks*512 for p in members)
        if size<200*1024:continue
        selected.append(root)
    checks=closure(selected)
    eligible=[r for r in selected if str(r) not in checks['blocked']]
    assert eligible
    base.mkdir(parents=True);stage.mkdir()
    for root in eligible:shutil.copytree(root,stage/root.name,copy_function=shutil.copy2)
    proof=fixture_archive.capture(stage,archive,base,compresslevel=1)
    files=json.loads((base/'fixture-inventory.json').read_text())['files']
    for root in eligible:
        expected={k[len(root.name)+1:]:v for k,v in files.items() if k.startswith(root.name+'/')}
        assert fixture_archive.inventory(root)==expected
    after=closure(eligible);assert not after['blocked'],after['blocked']
    report=dict(roots=[str(r) for r in eligible],skipped=skipped,closure_before=checks,closure_after=after,archive=str(archive),staging=str(stage),archive_verification=proof,utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Storage preservation only. Complete current regular-file bytes, original modes and mtimes; no test verdict changes or native test execution. Registered Git worktrees and live roots excluded.')
    (base/'capture.json').write_text(json.dumps(report,indent=2)+'\n')
    shutil.copyfile(__file__,base/'executed-storage.py')
    shutil.rmtree(stage)
    print(json.dumps(dict(roots=len(eligible),files=len(files),archive_bytes=proof['archive_bytes'],blocked=list(checks['blocked']))),flush=True)
if __name__=='__main__':capture()
