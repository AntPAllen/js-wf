#!/usr/bin/env python3
"""Retire only the worktree whose complete archive was verified and pushed."""
import datetime, hashlib, json, subprocess
from pathlib import Path
import boto3
from botocore.config import Config
base=Path(__file__).resolve().parent
repo=base.parents[2]
r=json.loads((base/'receipt.json').read_text())
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
remote_head=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert remote_head==head
assert subprocess.check_output(['git','show',head+':docs/scale/tmp-storage-review-2026-10-09/receipt.json'],cwd=repo)==(base/'receipt.json').read_bytes()
root=Path(r['root'])
assert root==Path('/tmp/js-wf-cas-baseline-topology')
assert subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip()==r['git_head']
actual={}
for p in root.rglob('*'):
 assert not p.is_symlink()
 if p.is_file():
  b=p.read_bytes();actual[str(p.relative_to(root))]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
assert actual==r['files']
s3=boto3.client('s3',endpoint_url='https://nameless-bird-8772.int.exe.xyz',aws_access_key_id='x',aws_secret_access_key='x',region_name='us-east-1',config=Config(s3={'addressing_style':'path'},connect_timeout=30,read_timeout=60))
b=s3.get_object(Bucket=r['bucket'],Key=r['key'])['Body'].read()
assert len(b)==r['archive_bytes'] and hashlib.sha256(b).hexdigest()==r['archive_sha256']
closure=json.loads(subprocess.check_output(['sudo','-n','python3',str(base/'check-inactive.py')],text=True))
assert not closure['process_or_mount_references'] and not closure['permission_errors']
archive=Path(r['local_archive'])
assert hashlib.sha256(archive.read_bytes()).hexdigest()==r['archive_sha256']
subprocess.run(['git','worktree','remove','--force',str(root)],cwd=repo,check=True)
archive.unlink()
report={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'recovery_receipt_pushed_at':head,'root_absent':not root.exists(),'transfer_archive_absent':not archive.exists(),'reclaimed_source_allocated_bytes':r['allocated_source_bytes'],'fresh_remote_sha256_verified':True,'closure':closure}
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
