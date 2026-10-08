import sys,json,subprocess,hashlib,shutil,datetime,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=repo/'docs/scale/tmp-storage-review-2026-10-08/small-leftovers'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(name):
 p=base/name;raw=subprocess.check_output(['git','cat-file','blob',head+':'+str(p.relative_to(repo))],cwd=repo);assert raw==p.read_bytes();return raw
meta=json.loads(committed('archive-verification.json'));ib=committed('fixture-inventory.json');inv=json.loads(ib);assert hashlib.sha256(ib).hexdigest()==meta['inventory_sha256']
c=json.loads(committed('capture.json'));receipt=json.loads(committed('s3-readback.json'));expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv
roots=[Path(r) for r in c['roots']];archive=Path(c['archive'])
for root in roots:
 expected_files={k[len(root.name)+1:]:v for k,v in inv['files'].items() if k.startswith(root.name+'/')}
 assert fixture_archive.inventory(root)==expected_files
with archive.open('rb') as f:assert s3.digest(f)==expected
closure=json.loads(subprocess.check_output(['sudo','-n','python3','/tmp/tmp-small-leftovers-20261008.py','closure'],text=True));assert not closure['blocked'] and not closure['permission_limits'],closure
allocated=0;seen=set()
for root in roots+[archive]:
 for path in [root,*root.rglob('*')] if root.is_dir() else [root]:
  st=path.lstat();key=st.st_dev,st.st_ino
  if key in seen:continue
  seen.add(key);assert not path.is_file() or st.st_nlink==1
  allocated+=st.st_blocks*512
report=dict(committed_pushed_head=head,complete_fresh_remote_verification=actual,privileged_closure=closure,reclaimed_allocated_bytes_including_archive=allocated,free_before=shutil.disk_usage('/tmp').free)
(base/'removal-pending.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-removal.py')
for root in roots:shutil.rmtree(root)
archive.unlink();report.update(free_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_selected_paths_absent=all(not p.exists() for p in roots+[archive]))
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(base/'removal-pending.json').unlink()
with (base/'README.md').open('a') as f:f.write('\nAll nine selected directories and the transfer archive were removed after committed/pushed receipts, fresh complete remote archive/member verification, unchanged originals, and privileged process/descriptor/container/mount/loop checks. S3 is the canonical preserved copy.\n')
print(json.dumps(dict(reclaimed_bytes=allocated,removed=report['all_selected_paths_absent'])))
