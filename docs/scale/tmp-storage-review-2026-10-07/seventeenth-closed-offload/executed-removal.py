import sys,json,hashlib,shutil,subprocess,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
config=s3.credentials()
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base=Path('docs/scale/tmp-storage-review-2026-10-07/seventeenth-closed-offload')
out=Path('/tmp/storage-seventeenth-removal-20261007');out.mkdir()
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report={'head':head,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[],'free_bytes_before':shutil.disk_usage('/tmp').free}
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def blob(p):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo)
 assert (repo/p).read_bytes()==b
 return b
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if s.startswith('worktree ')]
save()
for item in json.loads(blob(base/'selection.json'))['selected']:
 root=Path(item['root']);archive=Path(item['archive']);proof=base/root.name
 assert root.parent==Path('/tmp') and not root.is_symlink()
 assert not any(w.is_relative_to(root) or root.is_relative_to(w) for w in worktrees)
 assert json.loads(blob(proof/'origin.json'))==item
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'))
 invbytes=blob(proof/meta['inventory_file']);inv=json.loads(invbytes)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
 assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 checks=closure(root);assert fixture_archive.inventory(root)==inv['files']
 print('VERIFY_REMOTE',root.name,flush=True)
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 assert declared==inv and fixture_archive.inventory(root)==inv['files']
 assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
 with archive.open('rb') as stream:assert s3.digest(stream)==expected
 record={'root':str(root),'metadata':str(proof/'archive-verification.json'),'receipt':str(proof/'s3-readback.json'),'remote_every_member_and_hash_verified':actual,'closure_before':checks,'closure_after':closure(root),'removed_allocated_bytes':sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512,'archive_closure':closure(archive),'removed_staging_archive_allocated_bytes':archive.stat().st_blocks*512}
 if 'original_files' in item:
  byname=inv['files']
  record['original_files']=[]
  for selected in item['original_files']:
   original=Path(selected['path']);st=original.stat();linked=root/original.name
   assert original.parent==Path('/tmp') and not original.is_symlink()
   assert st.st_dev==selected['device'] and st.st_ino==selected['inode'] and st.st_nlink==2
   assert linked.stat().st_ino==st.st_ino and linked.stat().st_dev==st.st_dev
   entry=byname[original.name]
   assert st.st_size==entry['bytes']
   with original.open('rb') as stream:assert s3.digest(stream)=={'bytes':entry['bytes'],'sha256':entry['sha256']}
   record['original_files'].append({'path':str(original),'closure':closure(original),'removed_allocated_bytes':st.st_blocks*512})
  record['removed_allocated_bytes']=root.stat().st_blocks*512+sum(x['removed_allocated_bytes'] for x in record['original_files'])
 report['pending_verified_removal']=record;save()
 if 'original_files' in item:
  for selected in item['original_files']:Path(selected['path']).unlink()
 
 for directory in [root,*[p for p in root.rglob('*') if p.is_dir()]]:
  if not directory.stat().st_mode & 0o200: directory.chmod(directory.stat().st_mode | 0o200)
 shutil.rmtree(root);archive.unlink()
 report.pop('pending_verified_removal',None);report['removed'].append(record);save();print('REMOVED',root.name,flush=True)
report['reclaimed_original_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['removed_staging_archive_allocated_bytes']=sum(x['removed_staging_archive_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['reclaimed_original_allocated_bytes'],flush=True)
