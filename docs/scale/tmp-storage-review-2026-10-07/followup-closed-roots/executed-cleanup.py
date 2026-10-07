from pathlib import Path
import sys, os, json, shutil, subprocess, importlib.util, datetime
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure, fixture_archive, repo
base=repo/'docs/scale/tmp-storage-review-2026-10-07/followup-closed-roots'
roots=['js-wf-tier2-retained-blockdisk-smoke-20261005','js-wf-tier2-retained-blockdisk-ten-minute-20261005','js-wf-bulk-latency-full400k-valid-20261006','js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005']
def regular_inventory(root):
    files={}; links={}
    for p in sorted(root.rglob('*')):
        name=str(p.relative_to(root))
        if p.is_symlink(): links[name]=os.readlink(p)
        elif p.is_file():
            with p.open('rb') as f: rec=fixture_archive.digest(f)
            st=p.stat(); rec.update(mode=st.st_mode&0o777,mtime_ns=st.st_mtime_ns);files[name]=rec
        elif not p.is_dir(): raise ValueError(str(p))
    return files,links
mode=sys.argv[1]
if mode=='capture':
    base.mkdir(parents=True,exist_ok=False)
    shutil.copyfile(__file__,base/'executed-cleanup.py')
    shutil.copyfile('/tmp/storage_review_common.py',base/'executed-common.py')
    for name in roots:
        root=Path('/tmp')/name; out=base/name;out.mkdir()
        checks=closure(root);files,links=regular_inventory(root)
        worktrees=[]
        for line in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines():
            if line.startswith('worktree '):
                w=Path(line[9:])
                if w.exists() and w.is_relative_to(root):
                    assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=w)
                    worktrees.append({'path':str(w),'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip()})
        stage=Path('/tmp')/(name+'-cleanup-stage'); stage.mkdir()
        for rel in files:
            target=stage/rel;target.parent.mkdir(parents=True,exist_ok=True);os.link(root/rel,target)
        (stage/'__original_links_and_worktrees__.json').write_text(json.dumps({'links':links,'worktrees':worktrees,'restore_note':'Archive contains all regular bytes. Recreate listed symlinks explicitly; do not reuse archived .git pointer. Restore source as a standalone checkout or reconstruct worktree at recorded commit. Previously offloaded block media has its own existing S3 receipt.'},indent=2)+'\n')
        origin={'root':str(root),'stage':str(stage),'archive':str(Path('/tmp')/(name+'-cleanup.tar.gz')),'files':files,'links':links,'worktrees':worktrees,'closure':checks}
        (out/'origin.json').write_text(json.dumps(origin,indent=2)+'\n')
        fixture_archive.capture(stage,origin['archive'],out,compresslevel=1)
        assert regular_inventory(root)==(files,links)
        print('CAPTURED',name,flush=True)
elif mode=='retire':
    spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py'); s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
    config=s3.credentials();head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
    def committed(p):
        b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p.relative_to(repo))],cwd=repo)
        assert b==p.read_bytes();return json.loads(b)
    report={'head':head,'before_free_bytes':shutil.disk_usage('/tmp').free,'removed':[],'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    for name in roots:
        out=base/name;origin=committed(out/'origin.json');meta=committed(out/'archive-verification.json');inv=committed(out/'fixture-inventory.json');receipt=committed(out/'s3-readback.json')
        root=Path(origin['root']);stage=Path(origin['stage']);archive=Path(origin['archive'])
        expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
        assert receipt['archive']['full_readback']==expected
        with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
            p.stdin.write(config);p.stdin.close()
            try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
            except BaseException:p.kill();p.wait();raise
            err=p.stderr.read();assert p.wait()==0,err
        assert declared==inv and fixture_archive.inventory(stage)==inv['files']
        assert regular_inventory(root)==(origin['files'],origin['links'])
        checks={str(p):closure(p) for p in (root,stage,archive)}
        with archive.open('rb') as f:assert s3.digest(f)==expected
        for w in origin['worktrees']:
            assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=w['path'])
            assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=w['path'],text=True).strip()==w['head']
        record={'root':str(root),'full_remote_member_verification':actual,'closure':checks,'removed_worktrees':origin['worktrees']}
        report['pending']=record;(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
        for w in origin['worktrees']:subprocess.run(['git','worktree','remove',w['path']],cwd=repo,check=True)
        shutil.rmtree(root);shutil.rmtree(stage);archive.unlink()
        report.pop('pending');report['removed'].append(record);report['after_free_bytes']=shutil.disk_usage('/tmp').free
        (base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');print('REMOVED',name,flush=True)
    report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
else: raise ValueError(mode)
