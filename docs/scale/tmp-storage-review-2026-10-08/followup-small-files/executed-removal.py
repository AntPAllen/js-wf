import sys, json, hashlib, shutil, subprocess, datetime, importlib.util, os
from pathlib import Path
from collections import Counter
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
base=repo/'docs/scale/tmp-storage-review-2026-10-08/followup-small-files'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head
for name in ['capture.json','fixture-inventory.json','archive-verification.json','s3-readback.json']:
 assert subprocess.check_output(['git','cat-file','blob',head+':'+str((base/name).relative_to(repo))],cwd=repo)==(base/name).read_bytes()
inv=json.loads((base/'fixture-inventory.json').read_text())
meta=json.loads((base/'archive-verification.json').read_text())
capture=json.loads((base/'capture.json').read_text())
receipt=json.loads((base/'s3-readback.json').read_text())
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert receipt['archive']['full_readback']==expected
import boto3
s3=boto3.client('s3',endpoint_url=receipt['endpoint'],aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],region_name='us-east-1')
body=s3.get_object(Bucket=receipt['bucket'],Key=receipt['archive_key'])['Body']
try:declared,actual=fixture_archive.verify_hashed_stream(body,expected)
finally:body.close()
assert declared==inv
roots=[Path(p) for p in capture['roots']]
for p in roots:
 if p.is_dir():
  wanted={k[len(p.name)+1:]:v for k,v in inv['files'].items() if k.startswith(p.name+'/')}
  assert fixture_archive.inventory(p)==wanted,str(p)
 else:
  st=p.stat()
  assert dict(bytes=st.st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),mode=st.st_mode&0o777,mtime_ns=st.st_mtime_ns)==inv['files'][p.name],str(p)
closure=json.loads(subprocess.check_output(['sudo','-n','python3','/home/exedev/tmp-cleanup-followup.py','closure'],text=True))
assert not closure['blocked'] and not closure['permission_limits'],closure
archive=Path(capture['archive'])
with archive.open('rb') as stream:assert fixture_archive.digest(stream)==expected
paths=roots+[archive]
items=[p for root in paths for p in ([root,*root.rglob('*')] if root.is_dir() else [root])]
counts=Counter((p.stat().st_dev,p.stat().st_ino) for p in items if p.is_file())
seen=set();allocated=0;external=[]
for p in items:
 st=p.stat();key=st.st_dev,st.st_ino
 if key in seen:continue
 seen.add(key)
 if p.is_file() and st.st_nlink>counts[key]:external.append(str(p));continue
 allocated+=st.st_blocks*512
report=dict(pushed_receipt_revision=head,remote_complete_member_and_compressed_readback=actual,unchanged_originals=True,privileged_closure=closure,roots=len(roots),original_file_bytes=sum(v['bytes'] for v in inv['files'].values()),allocated_bytes_including_transfer_archive=allocated,external_hardlinks=external,tmp_allocated_before=subprocess.check_output(['sudo','-n','du','-sx','--block-size=1','/tmp'],text=True).strip())
(base/'removal-pending.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-removal.py')
for p in roots:
 shutil.rmtree(p) if p.is_dir() else p.unlink()
archive.unlink()
report.update(all_selected_paths_and_transfer_archive_absent=all(not p.exists() for p in paths),utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),tmp_allocated_after=subprocess.check_output(['sudo','-n','du','-sx','--block-size=1','/tmp'],text=True).strip())
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
(base/'removal-pending.json').unlink()
p=base/'README.md';p.write_text(p.read_text().replace('Local removal is pending S3 upload, complete readback verification, unchanged original inventories, and a fresh privileged usage check.','Local removal completed after the S3 receipt was committed and pushed, a fresh complete remote member and compressed-byte verification, unchanged original inventories, and a privileged usage check with no blocked paths or permission gaps. The transfer archive was also removed. The removal report records before/after allocated space; unrelated concurrent activity may affect that delta.'))
print(json.dumps({k:v for k,v in report.items() if k!='privileged_closure'}))
