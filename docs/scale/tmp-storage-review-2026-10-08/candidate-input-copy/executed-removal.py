import sys,json,hashlib,subprocess,shutil,datetime,os,importlib.util
from pathlib import Path
from collections import Counter
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
root=Path('/tmp/js-wf-candidate-partition200-input-20261007')
base=Path('docs/scale/tmp-storage-review-2026-10-08/candidate-input-copy')
proof=Path('docs/scale/lease-partition-component-2026-10-06/contiguous-component')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(path):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo);assert (repo/path).read_bytes()==b;return b
for name in ('restore.json','executed-restore.py'):assert (root/name).read_bytes()==committed(base/name)
assert set(p.name for p in root.iterdir())=={'component','restore.json','executed-restore.py'}
meta=json.loads(committed(proof/'archive-verification.json'));invbytes=committed(proof/'fixture-inventory.json');inv=json.loads(invbytes);receipt=json.loads(committed(proof/'s3-readback.json'))
assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv and fixture_archive.inventory(root/'component')==inv['files']
checks=closure(root)
members=[(p,p.lstat()) for p in [root,*root.rglob('*')]]
links=Counter((st.st_dev,st.st_ino) for p,st in members if p.is_file())
seen=set();allocated=0;external=[]
for p,st in members:
 identity=(st.st_dev,st.st_ino)
 if identity in seen:continue
 seen.add(identity)
 if p.is_file() and st.st_nlink>links[identity]:external.append(str(p));continue
 allocated+=st.st_blocks*512
assert fixture_archive.inventory(root/'component')==inv['files']
report=dict(head=head,root=str(root),verified_remote=actual,parent_proof=str(proof),closure=checks,allocated_bytes=allocated,externally_retained_hardlinks=external,allocation_accounting='Distinct device/inode blocks excluding external hardlinks; not physical extent ownership proof.',free_bytes_before=shutil.disk_usage('/tmp').free)
out=repo/base
(out/'pending.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.rmtree(root)
report.update(free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(out/'pending.json').unlink()
print(json.dumps(dict(removed=str(root),allocated_bytes=allocated,remote_verified=actual)),flush=True)
