import sys,json,hashlib,subprocess,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=Path('docs/scale/tier1-full125-2026-10-08/race-terminal');root=Path('/tmp/js-wf-tier1-full125-race-20261008');archive=Path('/tmp/js-wf-tier1-full125-race-complete-20261008.tar.gz')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def committed(name):
 p=base/name;b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo);assert (repo/p).read_bytes()==b;return b
meta=json.loads(committed('archive-verification.json'));invbytes=committed('fixture-inventory.json');inv=json.loads(invbytes);receipt=json.loads(committed('s3-readback.json'))
assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
config=s3.credentials()
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(config);p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv and fixture_archive.inventory(root)==inv['files']
with archive.open('rb') as f:assert s3.digest(f)==expected
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root/'source',text=True).strip()=='c13c8a647053dcaeb2312c26fa58e7e15c100945'
paths=[root,archive];checks={str(p):closure(p) for p in paths}
allocated={str(p):sum(q.lstat().st_blocks*512 for q in p.rglob('*'))+p.stat().st_blocks*512 if p.is_dir() else p.stat().st_blocks*512 for p in paths}
out=repo/'docs/scale/tier1-full125-2026-10-08/reclaimed';out.mkdir()
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report=dict(head=head,verified_remote=actual,closure=checks,allocated_bytes=allocated,free_bytes_before=shutil.disk_usage('/tmp').free)
(out/'pending.json').write_text(json.dumps(report,indent=2)+'\n')
subprocess.run(['git','worktree','remove',str(root/'source')],cwd=repo,check=True)
shutil.rmtree(root);archive.unlink()
report.update(reclaimed_allocated_bytes=sum(allocated.values()),free_bytes_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(out/'pending.json').unlink()
(out/'README.md').write_text('# Full125 race qualification fixture retired\n\nThe closed original fixture, full archive and registered clean source worktree were retired after fresh complete S3 byte/member verification, unchanged local inventories, committed/pushed receipt and visible process/descriptor/Docker/mount/loop closure checks. Permission limits are recorded. Reclaimed '+str(report['reclaimed_allocated_bytes'])+' allocated bytes. Test verdict and scope are unchanged. Restore the accepted full fixture to a fresh directory from its S3 receipt before reuse.\n')
print(json.dumps(dict(reclaimed_bytes=report['reclaimed_allocated_bytes'])),flush=True)
