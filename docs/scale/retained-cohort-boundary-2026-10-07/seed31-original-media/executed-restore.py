import sys,json,hashlib,subprocess,tarfile,datetime,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
sys.path.insert(0,'/tmp')
from storage_review_common import closure
import importlib.util
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=repo/'docs/scale/tmp-storage-review-2026-10-07/twenty-fourth-candidate-closed'
meta=json.loads((base/'archive-verification.json').read_text());inv=json.loads((base/'fixture-inventory.json').read_text());receipt=json.loads((base/'s3-readback.json').read_text())
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
selected={name:value for name,value in inv['files'].items() if name.startswith('seed-031/originals/') and '/streams/WF_INV/' in name}
root=Path('/tmp/js-wf-candidate-seed31-invocation-media-20261008');root.mkdir();shutil.copy2(__file__,root/'executed-restore.py')
class Hashed:
 def __init__(self,stream):self.stream=stream;self.hash=hashlib.sha256();self.size=0
 def read(self,size=-1):
  b=self.stream.read(size);self.hash.update(b);self.size+=len(b);return b
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close();hashed=Hashed(p.stdout);actual={};declared=None
 try:
  with tarfile.open(fileobj=hashed,mode='r|gz') as archive:
   for member in archive:
    name=fixture_archive.safe_name(member.name);assert member.isfile()
    stream=archive.extractfile(member)
    if name==fixture_archive.CONTROL:
     assert declared is None;declared=json.load(stream);continue
    assert name not in actual
    target=None
    if name in selected:
     destination=root/'selected'/name;destination.parent.mkdir(parents=True,exist_ok=True);target=destination.open('xb')
    h=hashlib.sha256();size=0
    try:
     while b:=stream.read(1<<20):
      h.update(b);size+=len(b)
      if target:target.write(b)
    finally:
     if target:target.close()
    actual[name]=dict(bytes=size,sha256=h.hexdigest(),mode=member.mode&0o777)
  while hashed.read(1<<20):pass
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert dict(bytes=hashed.size,sha256=hashed.hash.hexdigest())==expected
assert declared==inv and actual=={name:{k:v[k] for k in ['bytes','sha256','mode']} for name,v in inv['files'].items()}
for name,value in selected.items():
 target=root/'selected'/name
 with target.open('rb') as f:assert s3.digest(f)=={k:value[k] for k in ['bytes','sha256']}
 target.chmod(value['mode'])
 import os
 os.utime(target,ns=(value['mtime_ns'],value['mtime_ns']))
report=dict(original_receipt=str(base.relative_to(repo)/'s3-readback.json'),full_remote_archive_verified=expected,all_members_verified=len(actual),selected_files=selected,selected_bytes=sum(x['bytes'] for x in selected.values()),scope='Original failed seed31 WF_INV media selectively restored after complete remote compressed body/every-member verification. No broker reopened, native rerun, repair or qualification implied.',finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(root/'restore-proof.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(files=len(selected),bytes=report['selected_bytes'],full_archive_members=len(actual))),flush=True)
