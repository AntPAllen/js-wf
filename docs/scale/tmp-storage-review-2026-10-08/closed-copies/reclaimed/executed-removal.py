import sys,json,hashlib,subprocess,shutil,datetime,os,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
base=Path('docs/scale/tmp-storage-review-2026-10-08/closed-copies')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(path):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo);assert (repo/path).read_bytes()==b;return b
rows=json.loads(committed(base/'capture.json'))['fixtures']
rows.append(dict(root='/tmp/js-wf-tier1-full126-race-20261008',archive='/tmp/js-wf-tier1-full126-race-complete-20261008.tar.gz',canonical_metadata='docs/scale/tier1-full126-2026-10-08/race-terminal/archive-verification.json',source_revision='cf99bc0d3acc0fcec3ce639ee1804c1711db4d75'))
out=repo/base/'reclaimed';out.mkdir()
shutil.copyfile(__file__,out/'executed-removal.py')
report=dict(head=head,free_bytes_before=shutil.disk_usage('/tmp').free,removed=[])
for row in rows:
 root=Path(row['root']);archive=Path(row['archive']);proof=Path(row['canonical_metadata']).parent
 meta=json.loads(committed(proof/'archive-verification.json'));invbytes=committed(proof/'fixture-inventory.json');inv=json.loads(invbytes);receipt=json.loads(committed(proof/'s3-readback.json'))
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256'];assert receipt['archive']['full_readback']==expected
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(s3.credentials());p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 assert declared==inv and fixture_archive.inventory(root)==inv['files']
 with archive.open('rb') as f:assert s3.digest(f)==expected
 if row['source_revision']:
  assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
  assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root/'source',text=True).strip()==row['source_revision']
 checks={str(p):closure(p) for p in [root,archive]}
 allocated=sum(q.lstat().st_blocks*512 for q in root.rglob('*'))+root.stat().st_blocks*512+archive.stat().st_blocks*512
 pending=dict(root=str(root),archive=str(archive),closure=checks,verified_remote=actual,allocated_bytes=allocated)
 (out/'pending.json').write_text(json.dumps(pending,indent=2)+'\n')
 if row['source_revision']:subprocess.run(['git','worktree','remove',str(root/'source')],cwd=repo,check=True)
 shutil.rmtree(root);archive.unlink();report['removed'].append(pending)
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(out/'pending.json').unlink()
 print('RETIRED',root.name,allocated,flush=True)
report.update(reclaimed_allocated_bytes=sum(row['allocated_bytes'] for row in report['removed']),free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
print('TOTAL',report['reclaimed_allocated_bytes'],flush=True)
