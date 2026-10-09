#!/usr/bin/env python3
"""Preserve one inactive dirty worktree; removal is a separate step."""
import datetime, hashlib, io, json, subprocess, tarfile
from pathlib import Path
import boto3
from botocore.config import Config
root=Path('/tmp/js-wf-cas-baseline-topology')
base=Path(__file__).resolve().parent
archive=Path('/home/exedev/js-wf-cas-baseline-topology-20261009.tar.gz')
def inventory():
 result={}
 for p in sorted(root.rglob('*')):
  if p.is_symlink(): raise RuntimeError(f'Unexpected symlink: {p}')
  if p.is_file():
   b=p.read_bytes(); result[str(p.relative_to(root))]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
 return result
inv=inventory()
head=subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip()
status=subprocess.check_output(['git','-C',str(root),'status','--porcelain'],text=True)
(base/'uncommitted.patch').write_bytes(subprocess.check_output(['git','-C',str(root),'diff','--binary','HEAD']))
with tarfile.open(archive,'w:gz') as t: t.add(root,arcname=root.name)
assert inventory()==inv
raw=archive.read_bytes(); digest=hashlib.sha256(raw).hexdigest()
key=f'js-wf/tmp-cleanup/2026-10-09/cas-baseline-topology-{digest}.tar.gz'
s3=boto3.client('s3',endpoint_url='https://nameless-bird-8772.int.exe.xyz',aws_access_key_id='x',aws_secret_access_key='x',region_name='us-east-1',config=Config(s3={'addressing_style':'path'},connect_timeout=30,read_timeout=60,retries={'max_attempts':2}))
bucket='nameless-bird-8772'
s3.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/gzip')
remote=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
assert len(remote)==len(raw) and hashlib.sha256(remote).hexdigest()==digest
seen={}
with tarfile.open(fileobj=io.BytesIO(remote),mode='r:gz') as t:
 for m in t.getmembers():
  if m.isdir(): continue
  assert m.isfile()
  prefix=root.name+'/'
  assert m.name.startswith(prefix)
  b=t.extractfile(m).read()
  seen[m.name[len(prefix):]]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
assert seen==inv
assert inventory()==inv
receipt={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'root':str(root),'git_head':head,'git_status':status,'files':inv,'archive_bytes':len(raw),'archive_sha256':digest,'bucket':bucket,'key':key,'full_remote_readback_verified':True,'every_archive_file_verified':True,'allocated_source_bytes':sum(p.lstat().st_blocks*512 for p in [root,*root.rglob('*')]),'local_archive':str(archive),'removed':False}
(base/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({k:v for k,v in receipt.items() if k!='files'},indent=2))
