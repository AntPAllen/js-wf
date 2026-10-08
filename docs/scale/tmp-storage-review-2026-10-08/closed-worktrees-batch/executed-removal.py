import sys,json,subprocess,hashlib,shutil,datetime,importlib.util
from pathlib import Path
from collections import Counter
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=repo/'docs/scale/tmp-storage-review-2026-10-08/closed-worktrees-batch'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(name):
 p=base/name;raw=subprocess.check_output(['git','cat-file','blob',head+':'+str(p.relative_to(repo))],cwd=repo);assert raw==p.read_bytes();return raw
meta=json.loads(committed('archive-verification.json'));ib=committed('fixture-inventory.json');inv=json.loads(ib)
assert hashlib.sha256(ib).hexdigest()==meta['inventory_sha256']
c=json.loads(committed('capture.json'));receipt=json.loads(committed('s3-readback.json'))
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv
roots=list(map(Path,c['roots']));archive=Path(c['archive'])
for r in roots:assert fixture_archive.inventory(r)=={k[len(r.name)+1:]:v for k,v in inv['files'].items() if k.startswith(r.name+'/')},r
for wt in c['worktrees']:
 assert subprocess.check_output(['git','-C',wt['path'],'rev-parse','HEAD'],text=True).strip()==wt['head']
 assert not subprocess.check_output(['git','-C',wt['path'],'status','--porcelain'],text=True)
with archive.open('rb') as f:assert s3.digest(f)==expected
closure=json.loads(subprocess.check_output(['sudo','-n','python','/tmp/tmp-closed-worktrees-20261008.py','closure'],text=True));assert not closure['blocked'] and not closure['permission_limits'],closure
paths=roots+[archive];stats=[(q,q.lstat()) for p in paths for q in ([p,*p.rglob('*')] if p.is_dir() else [p])]
links=Counter((st.st_dev,st.st_ino) for q,st in stats if q.is_file());seen=set();allocated=0;external=[]
for q,st in stats:
 ident=st.st_dev,st.st_ino
 if ident in seen:continue
 seen.add(ident)
 if q.is_file() and st.st_nlink>links[ident]:external.append(str(q));continue
 allocated+=st.st_blocks*512
report=dict(committed_pushed_head=head,complete_fresh_remote_verification=actual,privileged_closure=closure,roots=c['roots'],allocation_bytes_including_transfer_archive=allocated,externally_retained_hardlinks=external,free_bytes_before=shutil.disk_usage('/tmp').free)
(base/'removal-pending.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-removal.py')
for wt in c['worktrees']:subprocess.run(['git','worktree','remove',wt['path']],cwd=repo,check=True)
for r in roots:
 if r.exists():shutil.rmtree(r)
archive.unlink()
report.update(free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_selected_roots_and_transfer_archive_absent=all(not p.exists() for p in paths))
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(base/'removal-pending.json').unlink()
with (base/'README.md').open('a') as f:f.write('\n## Local retirement completed\n\nAll six roots and the temporary archive were removed after committed and pushed S3 receipts, fresh full remote compressed-body and member validation, unchanged inventories and clean worktrees, and a privileged process/descriptor/container/mount/loop scan without permission gaps. Worktrees were removed using Git. S3 retains the complete archive; Git retains recovery metadata and file hashes.\n')
print(json.dumps(dict(roots=len(roots),allocated_bytes=allocated,all_absent=report['all_selected_roots_and_transfer_archive_absent'])))
