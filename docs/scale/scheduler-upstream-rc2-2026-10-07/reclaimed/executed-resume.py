import sys,json,hashlib,shutil,subprocess,datetime,stat
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
config=s3.credentials();head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base=Path('docs/scale/scheduler-upstream-rc2-2026-10-07');out=Path('/tmp/storage-scheduler-rc2-removal-20261007');report=json.loads((out/'removal.json').read_text())
assert not report['removed'] and report['pending_verified_removal']['root']=='/tmp/js-wf-upstream-scheduler-rc2-20261007'
shutil.copyfile(out/'executed-removal.py',out/'initial-executed-removal.py');shutil.copyfile(out/'removal.json',out/'initial-removal.json');shutil.copyfile(__file__,out/'executed-resume.py')
original_pending=report['pending_verified_removal'];report['resumed_head']=head;report['initial_failure']='PermissionError unlinking README.md in a closed copied upstream directory with read-only mode; fresh S3 verification and whole original inventory had passed before partial removal.'
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free;(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def blob(p):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo);assert (repo/p).read_bytes()==b;return b
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if s.startswith('worktree ')]
for index,item in enumerate(json.loads(blob(base/'retirement-selection.json'))['items']):
 root=Path(item['root']);archive=Path(item['archive']);proof=Path(item['proof'])
 assert root.parent==Path('/tmp') and not root.is_symlink() and not any(w.is_relative_to(root) or root.is_relative_to(w) for w in worktrees)
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'));invbytes=blob(proof/meta['inventory_file']);inv=json.loads(invbytes)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']};assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 before=closure(root);current=fixture_archive.inventory(root)
 if index==0:
  assert original_pending['metadata']==str(proof/'archive-verification.json') and original_pending['remote_every_member_and_hash_verified']==expected
  assert set(current)<=set(inv['files']) and all(v==inv['files'][n] for n,v in current.items())
 else:assert current==inv['files']
 print('VERIFY_REMOTE',root.name,flush=True)
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--max-time','300',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 assert declared==inv and fixture_archive.inventory(root)==current
 assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
 with archive.open('rb') as f:assert s3.digest(f)==expected
 record={'root':str(root),'metadata':str(proof/'archive-verification.json'),'receipt':str(proof/'s3-readback.json'),'remote_every_member_and_hash_verified':actual,'closure_before':before,'closure_after':closure(root),'archive_closure':closure(archive),'remaining_files_verified':len(current),'removed_allocated_bytes':original_pending['removed_allocated_bytes'] if index==0 else sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512,'removed_staging_archive_allocated_bytes':archive.stat().st_blocks*512}
 if index==0:record['initial_full_root_verification']=original_pending;record['partial_removal_missing_members']=sorted(set(inv['files'])-set(current))
 report['pending_verified_removal']=record;save()
 permissions=[]
 for path in [root,*[p for p in root.rglob('*') if p.is_dir()]]:
  assert not path.is_symlink() and path.resolve().is_relative_to(root.resolve())
  mode=stat.S_IMODE(path.stat().st_mode)
  if not mode & stat.S_IWUSR:
   path.chmod(mode|stat.S_IWUSR|stat.S_IXUSR);permissions.append({'path':str(path),'before_mode':mode,'after_mode':mode|stat.S_IWUSR|stat.S_IXUSR})
 record['closed_directory_permission_adjustments']=permissions;save()
 shutil.rmtree(root);archive.unlink();report.pop('pending_verified_removal',None);report['removed'].append(record);save();print('REMOVED',root.name,flush=True)
report['reclaimed_original_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed']);report['removed_staging_archive_allocated_bytes']=sum(x['removed_staging_archive_allocated_bytes'] for x in report['removed']);report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save();print('FINISHED',report['reclaimed_original_allocated_bytes'],report['removed_staging_archive_allocated_bytes'],flush=True)
