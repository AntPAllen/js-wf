from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   for link in ['cwd','root']:
    target=(p/link).resolve();assert not target.is_relative_to(root),(p,link,target)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 docker_retries=[]
 for attempt in range(5):
  result=subprocess.run(['docker','inspect',*ids],capture_output=True,text=True) if ids else None
  if result is None or result.returncode==0:
   running=json.loads(result.stdout) if result else [];break
  docker_retries.append(dict(attempt=attempt,ids=ids,error=result.stderr))
  ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 else:raise RuntimeError('Docker inspection did not stabilize after five fresh-list retries')
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids,docker_inspection_retries=docker_retries)


root=Path('/tmp/storage-seventh-proofs')
canonical=repo/'docs/scale/tmp-storage-review-2026-10-07/seventh-closed-roots'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
files=fixture_archive.inventory(root)
for name,row in files.items():
 local=root/name;tracked=canonical/name
 data=subprocess.check_output(['git','cat-file','blob',head+':'+str(tracked.relative_to(repo))],cwd=repo)
 assert hashlib.sha256(data).hexdigest()==row['sha256'] and len(data)==row['bytes'] and tracked.read_bytes()==data
observation=closure(root)
assert fixture_archive.inventory(root)==files
allocated=sum((root/name).stat().st_blocks*512 for name in files if (root/name).stat().st_nlink==1)
shutil.rmtree(root)
report=dict(head=head,root=str(root),files=len(files),allocated_bytes_reclaimed=allocated,verified_identical_to_pushed_canonical_files=True,closure=observation,scope='New metadata staging duplicates only; reclaimed bytes excluded from pre-existing-data total. Canonical Git inventories and S3 body receipts remain.')
(canonical/'metadata-staging-removal.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,canonical/'executed-metadata-staging-removal.py')
print(json.dumps(report,indent=2))
