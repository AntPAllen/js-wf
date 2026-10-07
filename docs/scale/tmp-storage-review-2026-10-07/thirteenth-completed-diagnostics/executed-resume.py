from pathlib import Path
import sys, subprocess, json, hashlib, shutil, importlib.util, datetime, os
sys.dont_write_bytecode = True
repo = Path('/home/exedev/js-wf')
sys.path.insert(0, str(repo/'scripts'))
import fixture_archive
def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, repo/'scripts'/filename)
    obj = importlib.util.module_from_spec(spec); spec.loader.exec_module(obj); return obj
s3 = module('s3', 'offload-proof-to-s3.py')
closed = module('closed', 'verify-tier2-closed-originals.py')
head = subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head == subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(path):
    data = subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo)
    assert (repo/path).read_bytes() == data
    return data
config = s3.credentials()
out = Path('/tmp/storage-current-closed-proof'); out.mkdir(exist_ok=True)
shutil.copyfile(__file__,out/'executed-resume.py')
report=json.loads((out/'removal.json').read_text())
report['resume_head']=head
def save():
    report['free_bytes_after'] = shutil.disk_usage('/tmp').free
    (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def closure(root):
    limits=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            assert not (p/'exe').resolve().is_relative_to(root)
            args=(p/'cmdline').read_bytes().split(b'\0')
            assert not any(str(root).encode() in arg for arg in args),(p,args)
            for link in ('cwd','root'):
                assert not (p/link).resolve().is_relative_to(root)
        except PermissionError: limits.append(str(p))
        except (FileNotFoundError,ProcessLookupError): pass
    ids=subprocess.check_output(['docker','ps','-aq'],text=True).split()
    containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
    for c in containers:
        for m in c['Mounts']:
            source=Path(m['Source']).resolve()
            assert not source.is_relative_to(root) and not root.is_relative_to(source)
    loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
    for device in loops['loopdevices']:
        assert not Path(device['back-file']).resolve().is_relative_to(root)
    mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
    def walk(rows):
        for row in rows:
            assert not Path(row['target']).resolve().is_relative_to(root)
            assert not row['source'].startswith(str(root))
            walk(row.get('children',[]))
    walk(mounts['filesystems'])
    return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),
                docker_ids_including_stopped=ids,loopdevices=loops,mounts=mounts)
def remote(url, expected):
    with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3',
        '--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
        p.stdin.write(config);p.stdin.close()
        try: result=fixture_archive.verify_hashed_stream(p.stdout,expected)
        except BaseException: p.kill();p.wait();raise
        error=p.stderr.read();assert p.wait()==0,error
    return result

items=json.loads(Path('/tmp/current-storage-qualified-candidates.json').read_text())
report['skipped']=[]
worktrees=[Path(line.removeprefix('worktree ')) for line in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if line.startswith('worktree ')]
save()
for item in items:
    root=Path(item['root'])
    if any(r['root']==str(root) for r in report['removed']):continue
    if '400k' in root.name or 'million' in root.name:
        report['skipped'].append(dict(root=str(root),reason='Reusable scale fixture'));save();continue
    if any(w.is_relative_to(root) for w in worktrees):
        report['skipped'].append(dict(root=str(root),reason='Registered Git worktree retained'));save();continue
    try:
        proof=Path(item['metadata']).parent
        meta=json.loads(blob(proof/'archive-verification.json'))
        receipt=json.loads(blob(Path(item['receipt'])))
        inventory_bytes=blob(proof/meta['inventory_file']); inventory=json.loads(inventory_bytes)
        assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
        assert receipt['canonical_metadata']==item['metadata']
        expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
        assert receipt['archive']['full_readback']==expected
        before=closure(root)
        current=fixture_archive.inventory(root)
        pending=report.get('pending_verified_removal',{})
        if pending.get('root')==str(root):
            assert all(inventory['files'].get(k)==v for k,v in current.items()),'remaining files changed after interrupted removal'
        else:assert current==inventory['files'],'current inventory changed'
        print('VERIFY_REMOTE',root.name,flush=True)
        declared,actual=remote(receipt['archive']['url'],expected)
        assert declared==inventory
        assert fixture_archive.inventory(root)==current
        after=closure(root)
        allocated=sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512
        record=dict(root=str(root),canonical_metadata=item['metadata'],receipt=item['receipt'],archive_url=receipt['archive']['url'],full_remote_archive_and_every_member_verified=actual,files=len(inventory['files']),local_complete_tree_matches_committed_inventory=True,closure_before=before,closure_immediately_before_removal=after,removed_allocated_bytes=allocated,interrupted_prior_removal=report.get('pending_verified_removal',{}).get('root')==str(root))
        if record['interrupted_prior_removal']:
            record['removed_allocated_bytes']=report['pending_verified_removal']['removed_allocated_bytes']
        report['pending_verified_removal']=record;save()
        for directory in [root,*[p for p in root.rglob('*') if p.is_dir()]]:
            directory.chmod(directory.stat().st_mode | 0o700)
        shutil.rmtree(root)
        assert not root.exists()
        archive=Path(str(root)+'.tar.gz')
        if archive.exists():
            assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
            with archive.open('rb') as f:assert s3.digest(f)==expected
            record['archive_closure']=closure(archive)
            record['removed_allocated_bytes']+=archive.stat().st_blocks*512
            archive.unlink();record['removed_archive']=str(archive)
        report.pop('pending_verified_removal',None);report['removed'].append(record);save()
        print('REMOVED',root.name,record['removed_allocated_bytes'],flush=True)
    except (AssertionError,ValueError) as error:
        report['skipped'].append(dict(root=str(root),reason=str(error)));save();print('RETAINED',root.name,str(error),flush=True)
report['removed_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['removed_allocated_bytes'],'free',report['free_bytes_after'],flush=True)
