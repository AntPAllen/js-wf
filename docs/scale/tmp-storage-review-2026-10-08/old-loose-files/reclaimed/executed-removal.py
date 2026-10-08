import sys,json,hashlib,subprocess,shutil,datetime,os,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
base=Path('docs/scale/tmp-storage-review-2026-10-08/old-loose-files')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(name):
 p=base/name;b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo);assert (repo/p).read_bytes()==b;return b
plan=json.loads(committed('capture.json'));meta=json.loads(committed('archive-verification.json'));invbytes=committed('fixture-inventory.json');inv=json.loads(invbytes);receipt=json.loads(committed('s3-readback.json'))
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected;assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
stage=Path(plan['stage']);archive=Path(plan['archive'])
assert declared==inv and fixture_archive.inventory(stage)==inv['files']
with archive.open('rb') as f:assert s3.digest(f)==expected
checks={str(p):closure(p) for p in [stage,archive]};allocated=archive.stat().st_blocks*512+stage.stat().st_blocks*512
for row in plan['selected']:
 p=Path(row['path']);stat=p.stat();assert stat.st_ino==row['inode'] and stat.st_dev==row['device'] and stat.st_nlink==2
 assert (stage/p.name).stat().st_ino==stat.st_ino
 checks[str(p)]=closure(p);allocated+=stat.st_blocks*512
assert fixture_archive.inventory(stage)==inv['files']
out=repo/base/'reclaimed';out.mkdir();shutil.copyfile(__file__,out/'executed-removal.py')
report=dict(head=head,verified_remote=actual,closure=checks,reclaimed_allocated_bytes=allocated,free_bytes_before=shutil.disk_usage('/tmp').free,removed_original_paths=[row['path'] for row in plan['selected']],hardlink_staging_not_double_counted=True)
(out/'pending.json').write_text(json.dumps(report,indent=2)+'\n')
for row in plan['selected']:Path(row['path']).unlink()
shutil.rmtree(stage);archive.unlink()
report.update(finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_after=shutil.disk_usage('/tmp').free)
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(out/'pending.json').unlink()
print('RETIRED_LOOSE',len(plan['selected']),allocated,flush=True)
