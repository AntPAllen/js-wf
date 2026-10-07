import sys,pathlib,json,subprocess,hashlib,shutil,datetime,os
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
repo=pathlib.Path('/home/exedev/js-wf');out=repo/'docs/scale/tmp-storage-review-2026-10-07/twenty-second-tier1-reclaimed'
items=[(pathlib.Path('/tmp/js-wf-blob-publication-tier1-20261007'),pathlib.Path('/tmp/js-wf-blob-publication-tier1-20261007.tar.gz'),repo/'docs/scale/blob-publication-tier1-2026-10-07'),(pathlib.Path('/tmp/js-wf-tier1-full123-normal100k-20261007'),pathlib.Path('/tmp/js-wf-tier1-full123-normal100k-complete-20261007.tar.gz'),repo/'docs/scale/tier1-full123-2026-10-07/normal100k-accepted')]
report={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[],'scope':'Closed complete Tier1 fixtures only; actual verdicts and every file preserved in verified S3 archives. Candidate200 remains live and untouched.'}
for root,archive,canonical in items:
 meta=json.loads((canonical/'archive-verification.json').read_text());manifest=json.loads((canonical/'fixture-inventory.json').read_text());receipt=json.loads((canonical/'s3-readback.json').read_text());expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
 assert fixture_archive.inventory(root)==manifest['files']
 with archive.open('rb') as stream:assert fixture_archive.verify_hashed_stream(stream,expected)[0]==manifest
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(b'user = "x:x"\n');p.stdin.close();remote,digest=fixture_archive.verify_hashed_stream(p.stdout,expected);error=p.stderr.read();assert p.wait()==0,error
 assert remote==manifest
 report['removed'].append({'root':str(root),'archive':str(archive),'canonical':str(canonical.relative_to(repo)),'remote_every_member_verified':meta['members'],'remote_digest':digest,'root_closure':closure(root),'archive_closure':closure(archive)})
aux=pathlib.Path('/tmp/js-wf-blob-publication-traces-20261007')
for p in aux.iterdir():assert p.read_bytes()==(repo/'sim/testdata/regressions'/('blob-publication-'+p.name)).read_bytes()
auxproof=pathlib.Path('/tmp/js-wf-blob-publication-tier1-20261007-proof')
for p in auxproof.iterdir():assert p.read_bytes()==(repo/'docs/scale/blob-publication-tier1-2026-10-07'/p.name).read_bytes()
report['auxiliary_closure']={str(p):closure(p) for p in [aux,auxproof]}
# Recheck every donor before deleting any root. No original fixture is reopened.
for root,archive,canonical in items:assert fixture_archive.inventory(root)==json.loads((canonical/'fixture-inventory.json').read_text())['files']
paths=[p for root,archive,_ in items for p in [root,archive]]+[aux,auxproof]
files=[f for p in paths for f in ([p] if p.is_file() else p.rglob('*')) if f.is_file()]
inodes={}
for f in files:
 stat=f.stat();entry=inodes.setdefault((stat.st_dev,stat.st_ino),{'links':stat.st_nlink,'removed_links':0,'allocated':stat.st_blocks*512});entry['removed_links']+=1
allocated=sum(v['allocated'] for v in inodes.values() if v['links']==v['removed_links'])
source=items[1][0]/'source';assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
subprocess.run(['git','worktree','remove',str(source)],cwd=repo,check=True)
for p in paths:
 if p.is_file():p.unlink()
 else:shutil.rmtree(p)
report['reclaimed_allocated_bytes']=allocated;report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['filesystem']=subprocess.check_output(['df','-h','/tmp'],text=True)
(out/'cleanup.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'reclaimed_allocated_bytes':allocated,'removed':[str(p) for p in paths]}),flush=True)
