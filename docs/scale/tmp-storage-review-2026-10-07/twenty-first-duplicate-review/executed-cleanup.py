import sys,json,pathlib,subprocess,hashlib,shutil,datetime,importlib.util
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
repo=pathlib.Path('/home/exedev/js-wf')
out=repo/'docs/scale/tmp-storage-review-2026-10-07/twenty-first-duplicate-review'
canonical=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07/terminal-failure'
archive=pathlib.Path('/tmp/js-wf-parallel-recovery-journal-24h-complete-20261007.tar.gz')
watch=pathlib.Path('/tmp/js-wf-parallel-recovery-journal-24h-watch-20261007')
spec=importlib.util.spec_from_file_location('offload',repo/'scripts/offload-proof-to-s3.py');offload=importlib.util.module_from_spec(spec);spec.loader.exec_module(offload)
manifest=json.loads((canonical/'fixture-inventory.json').read_text())
metadata=json.loads((canonical/'archive-verification.json').read_text())
receipt=json.loads((canonical/'s3-readback.json').read_text())
expected={'bytes':metadata['archive_bytes'],'sha256':metadata['archive_sha256']}
report={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'canonical_metadata':str(canonical.relative_to(repo)/'archive-verification.json'),'scope':'Duplicate storage removal only; original failed verdict preserved; running campaigns untouched.','removed':[]}
# Read every old member and compressed byte, including trailing data.
with archive.open('rb') as stream:
 old_digest=offload.digest(stream)
with archive.open('rb') as stream:
 old_manifest,_=fixture_archive.verify_hashed_stream(stream,old_digest)
assert all(manifest['files'].get(n)==v for n,v in old_manifest['files'].items())
watch_inventory=fixture_archive.inventory(watch)
for name,value in watch_inventory.items():
 mapped='actual-server-executables/'+name.split('/',1)[1] if name.startswith('server-executables/') else 'watch-'+name
 assert manifest['files'].get(mapped)==value,(name,mapped)
# Fresh full S3 byte and member readback, without making another local archive.
config=b'user = "x:x"\n'
base=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800']
with subprocess.Popen(base+[receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as process:
 process.stdin.write(config);process.stdin.close()
 remote,remote_digest=fixture_archive.verify_hashed_stream(process.stdout,expected)
 error=process.stderr.read();assert process.wait()==0,error
assert remote==manifest
report['fresh_remote_readback']={'url':receipt['archive']['url'],'digest':remote_digest,'verified_members':len(remote['files'])+1,'all_old_files_identical_in_remote':True,'old_archive':old_digest,'old_files':len(old_manifest['files']),'observer_files':len(watch_inventory)}
# Check both donors before any removal. Permission limits are retained in evidence.
report['closure']={str(p):closure(p) for p in [archive,watch]}
with archive.open('rb') as stream:assert offload.digest(stream)==old_digest
assert fixture_archive.inventory(watch)==watch_inventory
report['removed'].append({'path':str(archive),'allocated_bytes':archive.stat().st_blocks*512});archive.unlink()
allocated=sum(p.stat().st_blocks*512 for p in watch.rglob('*') if p.is_file())
shutil.rmtree(watch);report['removed'].append({'path':str(watch),'allocated_bytes':allocated})
report['reclaimed_allocated_bytes']=sum(x['allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(out/'cleanup.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'reclaimed_allocated_bytes':report['reclaimed_allocated_bytes'],'removed':[x['path'] for x in report['removed']]}),flush=True)
