import sys,json,subprocess,hashlib,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=Path('docs/scale/tmp-storage-review-2026-10-07/twenty-third-closed-batch')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(name):
 p=base/name;data=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo);assert (repo/p).read_bytes()==data;return data
meta=json.loads(committed('archive-verification.json'));invbytes=committed('fixture-inventory.json');inv=json.loads(invbytes)
assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
receipt=json.loads(committed('s3-readback.json'));origins=json.loads(committed('origins.json'))
expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
assert receipt['archive']['full_readback']==expected
config=s3.credentials()
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(config);p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv
stage=Path('/tmp/closed-tmp-batch-stage-20261007');archive=Path('/tmp/closed-tmp-batch-20261007.tar.gz')
assert fixture_archive.inventory(stage)==inv['files'];closure(stage);closure(archive)
with archive.open('rb') as f:assert s3.digest(f)==expected
report={'head':head,'remote_every_member_and_complete_hash':actual,'before_free_bytes':shutil.disk_usage('/tmp').free,'removed':[]}
out=repo/base
shutil.copyfile(__file__,out/'executed-removal.py')
for row in origins['selected']:
 root=Path(row['root']);check=closure(root)
 assert fixture_archive.inventory(root)==row['inventory']
 for name in row['inventory']:
  source=root/name;target=stage/root.name/name
  assert source.samefile(target) and source.stat().st_nlink==2
 report['pending']=dict(root=str(root),closure=check,allocated_bytes=row['allocated_bytes'])
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
 shutil.rmtree(root);report['removed'].append(report.pop('pending'))
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
print('ORIGINALS_REMOVED',len(report['removed']),flush=True)
assert fixture_archive.inventory(stage)==inv['files'];closure(stage);shutil.rmtree(stage)
closure(archive);report['archive_allocated_bytes']=archive.stat().st_blocks*512;archive.unlink()
report['reclaimed_original_allocated_bytes']=sum(r['allocated_bytes'] for r in report['removed'])
report['after_free_bytes']=shutil.disk_usage('/tmp').free
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ('reclaimed_original_allocated_bytes','archive_allocated_bytes','after_free_bytes')}),flush=True)
