import sys,json,hashlib,shutil,subprocess,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
config=s3.credentials()
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base=Path('docs/scale/tmp-storage-review-2026-10-07/fifteenth-scale-offload')
out=Path('/tmp/remaining-tmp-removal-20261007');out.mkdir()
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report={'head':head,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[],'free_bytes_before':shutil.disk_usage('/tmp').free}
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def blob(p):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo)
 assert (repo/p).read_bytes()==b
 return b
items=[(base/'million-capacity',None),(base/'blockdisk-smoke',None),(base/'blockdisk-ten-minute',None),(base/'top-level-files','/tmp/closed-topfiles-stage-20261007'),(Path('docs/scale/million-timer-terminal-2026-10-02/complete-primary-root-2026-10-06'),'/tmp/js-wf-timer-volume-million-service-20261001')]
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if s.startswith('worktree ')]
save()
for proof,override in items:
 root=Path(override or json.loads(blob(proof/'origin.json'))['root'])
 assert not any(w.is_relative_to(root) or root.is_relative_to(w) for w in worktrees)
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'))
 invbytes=blob(proof/meta['inventory_file']);inv=json.loads(invbytes)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
 assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 checks=closure(root);assert fixture_archive.inventory(root)==inv['files']
 print('VERIFY_REMOTE',proof,flush=True)
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 assert declared==inv and fixture_archive.inventory(root)==inv['files']
 record={'root':str(root),'metadata':str(proof/'archive-verification.json'),'receipt':str(proof/'s3-readback.json'),'remote_every_member_and_hash_verified':actual,'closure_before':checks,'closure_after':closure(root),'removed_allocated_bytes':0}
 if proof.name=='top-level-files':
  individual=[]
  for name,entry in inv['files'].items():
   original=Path('/tmp')/name;copy=root/name
   assert not original.is_symlink() and original.samefile(copy) and original.stat().st_nlink==2
   with original.open('rb') as stream:assert s3.digest(stream)=={k:entry[k] for k in ('bytes','sha256')}
   individual.append({'path':str(original),'closure':closure(original),'allocated_bytes':original.stat().st_blocks*512})
  record['original_files']=individual
  record['removed_allocated_bytes']=sum(x['allocated_bytes'] for x in individual)+root.stat().st_blocks*512
  report['pending_verified_removal']=record;save()
  for item in individual:Path(item['path']).unlink()
  shutil.rmtree(root)
 else:
  record['removed_allocated_bytes']=sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512
  report['pending_verified_removal']=record;save();shutil.rmtree(root)
  if proof.name.startswith('blockdisk-'):
   (root.parent/'BLOCK_MEDIA_MOVED_TO_S3.json').write_text(json.dumps({'removed':str(root),'receipt':str(proof/'s3-readback.json'),'restore':'Restore to fresh directory, recreate empty store directory, attach documented block fixture only before reuse. Existing node-2 symlink retained; no process started.'},indent=2)+'\n')
 archive=Path('/tmp')/('remaining-'+proof.name+'-20261007.tar.gz')
 if archive.exists():
  assert not archive.is_symlink() and archive.stat().st_nlink==1
  with archive.open('rb') as stream:assert s3.digest(stream)==expected
  record['archive_closure']=closure(archive);record['removed_staging_archive']=str(archive)
  record['removed_staging_archive_allocated_bytes']=archive.stat().st_blocks*512
  archive.unlink()
 report.pop('pending_verified_removal',None);report['removed'].append(record);save();print('REMOVED',root,flush=True)
report['reclaimed_original_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['removed_staging_archive_allocated_bytes']=sum(x.get('removed_staging_archive_allocated_bytes',0) for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['reclaimed_original_allocated_bytes'],flush=True)
