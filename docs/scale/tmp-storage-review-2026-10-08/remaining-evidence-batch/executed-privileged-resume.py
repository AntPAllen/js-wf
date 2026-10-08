import sys,json,subprocess,hashlib,shutil,datetime,importlib.util
from pathlib import Path
from collections import Counter
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('batch','/tmp/cleanup-evidence-storage-20261008.py');b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
spec=importlib.util.spec_from_file_location('s3',b.repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=b.repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=b.repo,text=True).split()[0]
def committed(name):
    p=b.base/name
    raw=subprocess.check_output(['git','cat-file','blob',head+':'+str(p.relative_to(b.repo))],cwd=b.repo)
    assert raw==p.read_bytes();return raw
meta=json.loads(committed('archive-verification.json'));ib=committed('fixture-inventory.json');inv=json.loads(ib)
assert hashlib.sha256(ib).hexdigest()==meta['inventory_sha256']
c=json.loads(committed('capture.json'));receipt=json.loads(committed('s3-readback.json'))
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert receipt['archive']['full_readback']==expected
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
    p.stdin.write(s3.credentials());p.stdin.close()
    try:declared,actual=b.fixture_archive.verify_hashed_stream(p.stdout,expected)
    except BaseException:p.kill();p.wait();raise
    error=p.stderr.read();assert p.wait()==0,error
assert declared==inv
roots=[Path(r) for r in c['roots']]
report=json.loads((b.base/'removal-pending.json').read_text())
assert report['committed_pushed_head']==head
remaining=[r for r in roots if r.exists()]
report['absent_on_privileged_resume']=len(roots)-len(remaining)
report['resume_reason']='Unprivileged removal encountered a root-owned sticky-directory file; already checked entries were partly removed. Privileged resume repeats complete remote verification, unchanged remaining inventories and closure.'
for r in remaining:
    wanted={k[len(r.name)+1:]:v for k,v in inv['files'].items() if k.startswith(r.name+'/')}
    if r.is_dir():assert b.fixture_archive.inventory(r)==wanted,str(r)
    else:
        st=r.stat()
        assert dict(bytes=st.st_size,sha256=hashlib.sha256(r.read_bytes()).hexdigest(),mode=st.st_mode & 0o777,mtime_ns=st.st_mtime_ns)==inv['files'][r.name],str(r)
privileged=b.closure(roots+[b.archive])
assert not privileged['blocked'] and not privileged['permission_limits']
report['privileged_resume_closure']=privileged
report['complete_fresh_remote_verification_on_resume']=actual
allocated=report['allocation_bytes_including_local_archive']
paths=roots+[b.archive]
shutil.copyfile(__file__,b.base/'executed-privileged-resume.py')
for r in remaining:
    if r.is_dir():shutil.rmtree(r)
    else:r.unlink()
b.archive.unlink()
report.update(free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_selected_roots_and_local_archive_absent=all(not p.exists() for p in paths))
(b.base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(b.base/'removal-pending.json').unlink()
p=b.base/'README.md';p.write_text(p.read_text()+'\n## Local retirement completed\n\nAll 3,989 selected original entries and the local transfer archive were removed after the receipt was committed and pushed, fresh complete remote compressed-byte/member verification, unchanged original inventories and a privileged process/descriptor/container/mount/loop check with no permission gaps. The removal report records '+str(allocated)+' allocated bytes including the temporary transfer archive. S3 is the canonical archive; Git retains its complete inventory and recovery receipt.\n')
print(json.dumps(dict(roots=len(roots),allocated_bytes=allocated,free_delta=report['free_bytes_after']-report['free_bytes_before'])),flush=True)
