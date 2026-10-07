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
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
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
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)

head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
config=s3.credentials()
def get(url,verify):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try: result=verify(p.stdout)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 return result
out=Path('/tmp/storage-seventh-staging-proof')
out.mkdir(exist_ok=False)
reports=[]
for item in json.loads(Path('/tmp/storage-seventh-staging-plan.json').read_text()):
 path=Path(item['path']);receipt=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+item['receipt']],cwd=repo))
 meta_path=receipt['canonical_metadata'];meta=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+meta_path],cwd=repo))
 inventory_path=str(Path(meta_path).parent/meta['inventory_file'])
 inventory_bytes=subprocess.check_output(['git','cat-file','blob',head+':'+inventory_path],cwd=repo)
 expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
 assert item['sha256']==expected['sha256'] and item['bytes']==expected['bytes']
 with path.open('rb') as stream:assert s3.digest(stream)==expected
 remote,actual=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected))
 assert remote==json.loads(inventory_bytes)
 assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
 observed=closure(path)
 assert path.is_file() and not path.is_symlink() and path.stat().st_nlink==1
 allocated=path.stat().st_blocks*512
 path.unlink()
 reports.append(dict(path=str(path),receipt=item['receipt'],archive_url=receipt['archive']['url'],verified_full_remote_archive_and_members=actual,closure=observed,staging_bytes_removed=allocated))
 (out/'removal.json').write_text(json.dumps(dict(head=head,scope='Verified local archive staging copies only; original fixture roots unchanged.',reports=reports),indent=2)+'\n')
 print('REMOVED_STAGING',path.name,allocated,flush=True)
shutil.copyfile(__file__,out/'executed-removal.py')
