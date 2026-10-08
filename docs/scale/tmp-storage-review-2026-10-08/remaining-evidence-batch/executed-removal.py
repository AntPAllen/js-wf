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
for r in roots:
    wanted={k[len(r.name)+1:]:v for k,v in inv['files'].items() if k.startswith(r.name+'/')}
    if r.is_dir():assert b.fixture_archive.inventory(r)==wanted,str(r)
    else:
        st=r.stat()
        assert dict(bytes=st.st_size,sha256=hashlib.sha256(r.read_bytes()).hexdigest(),mode=st.st_mode & 0o777,mtime_ns=st.st_mtime_ns)==inv['files'][r.name],str(r)
with b.archive.open('rb') as f:assert s3.digest(f)==expected
privileged=json.loads(subprocess.check_output(['sudo','-n','python','/tmp/cleanup-evidence-closure-20261008.py']))
assert not privileged['blocked'] and not privileged['permission_limits']
paths=roots+[b.archive]
members=[(p,q,q.lstat()) for p in paths for q in ([p,*p.rglob('*')] if p.is_dir() else [p])]
links=Counter((st.st_dev,st.st_ino) for _,q,st in members if q.is_file())
seen=set();allocated=0;external=[]
for owner,q,st in members:
    identity=st.st_dev,st.st_ino
    if identity in seen:continue
    seen.add(identity)
    if q.is_file() and st.st_nlink>links[identity]:external.append(str(q));continue
    allocated+=st.st_blocks*512
report=dict(committed_pushed_head=head,complete_fresh_remote_verification=actual,privileged_closure=privileged,roots=c['roots'],allocation_bytes_including_local_archive=allocated,externally_retained_hardlinks=external,accounting='Distinct device/inode allocated blocks, excluding externally retained hardlinks; free-space delta can include concurrent activity.',free_bytes_before=shutil.disk_usage('/tmp').free)
(b.base/'removal-pending.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,b.base/'executed-removal.py');shutil.copyfile('/tmp/cleanup-evidence-closure-20261008.py',b.base/'executed-privileged-closure.py')
for r in roots:
    if r.is_dir():shutil.rmtree(r)
    else:r.unlink()
b.archive.unlink()
report.update(free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_selected_roots_and_local_archive_absent=all(not p.exists() for p in paths))
(b.base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(b.base/'removal-pending.json').unlink()
p=b.base/'README.md';p.write_text(p.read_text()+'\n## Local retirement completed\n\nAll 3,989 selected original entries and the local transfer archive were removed after the receipt was committed and pushed, fresh complete remote compressed-byte/member verification, unchanged original inventories and a privileged process/descriptor/container/mount/loop check with no permission gaps. The removal report records '+str(allocated)+' allocated bytes including the temporary transfer archive. S3 is the canonical archive; Git retains its complete inventory and recovery receipt.\n')
print(json.dumps(dict(roots=len(roots),allocated_bytes=allocated,free_delta=report['free_bytes_after']-report['free_bytes_before'])),flush=True)
