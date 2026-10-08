import sys,json,hashlib,subprocess,shutil,datetime,os,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
base=Path('docs/scale/corrected-partition200-2026-10-08/seed001-independent')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(path):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo);assert (repo/path).read_bytes()==b;return b
rows=[(base,Path('/tmp/js-wf-corrected-partition200-20261008/campaign/seed-001'),Path('/tmp/js-wf-corrected-partition-seed001-complete-20261008.tar.gz'),False),(base/'reviewer-artifacts',Path('/tmp/js-wf-corrected-partition200-seed001-independent-v2-20261008'),Path('/tmp/js-wf-corrected-partition-seed001-reviewer-20261008.tar.gz'),True),(base/'initial-reviewer-preparation',Path('/tmp/js-wf-corrected-partition200-seed001-independent-20261008'),Path('/tmp/js-wf-corrected-partition-seed001-reviewer-preparation-20261008.tar.gz'),True)]
removed=[]
for proof,root,archive,remove_root in rows:
 meta=json.loads(committed(proof/'archive-verification.json'));invbytes=committed(proof/'fixture-inventory.json');inv=json.loads(invbytes);receipt=json.loads(committed(proof/'s3-readback.json'));expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256'] and receipt['archive']['full_readback']==expected
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(s3.credentials());p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 assert declared==inv and fixture_archive.inventory(root)==inv['files']
 with archive.open('rb') as f:assert s3.digest(f)==expected
 paths=[archive]+([root] if remove_root else [])
 checks={str(p):closure(p) for p in paths}
 allocated=sum(sum(q.lstat().st_blocks*512 for q in p.rglob('*'))+p.stat().st_blocks*512 if p.is_dir() else p.stat().st_blocks*512 for p in paths)
 removed.append(dict(proof=str(proof),paths=[str(p) for p in paths],closure=checks,remote=actual,allocated_bytes=allocated,root_retained=not remove_root))
# Check every donor before the first mutation.
for proof,root,archive,remove_root in rows:
 assert fixture_archive.inventory(root)==json.loads(committed(proof/'fixture-inventory.json'))['files']
 for p in [archive]+([root] if remove_root else []):closure(p)
out=repo/base/'redundant-reclaimed';out.mkdir();shutil.copyfile(__file__,out/'executed-removal.py');report=dict(head=head,removed=removed,free_bytes_before=shutil.disk_usage('/tmp').free,native_original_retained_for_full_campaign_review=True)
(out/'pending.json').write_text(json.dumps(report,indent=2)+'\n')
for proof,root,archive,remove_root in rows:
 archive.unlink()
 if remove_root:shutil.rmtree(root)
report.update(reclaimed_allocated_bytes=sum(r['allocated_bytes'] for r in removed),free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(out/'pending.json').unlink()
print('RETIRED_REDUNDANT_SEED001_COPIES',report['reclaimed_allocated_bytes'],flush=True)
