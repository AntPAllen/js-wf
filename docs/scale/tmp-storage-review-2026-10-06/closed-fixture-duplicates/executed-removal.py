from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
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

config=s3.credentials()
def get(url,verify):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try: result=verify(p.stdout)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 return result

head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
blob=lambda p:subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo)
plan=json.loads(Path('/tmp/storage-verified-plan.json').read_text()); assert plan['head']==head
out=repo/'docs/scale/tmp-storage-review-2026-10-06/closed-fixture-duplicates'
assert not (out/'removal.json').exists()
reports=[];staging_reports=[];verified={}
def remote(rp):
 if rp in verified:return verified[rp]
 receipt=json.loads(blob(rp));mp=Path(receipt['canonical_metadata']);meta=json.loads(blob(mp));ip=mp.parent/meta['inventory_file'];invbytes=blob(ip)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 for path,key in [(mp,'metadata'),(ip,'inventory')]:
  data=blob(path);assert get(receipt[key]['url'],s3.digest)==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 inv,actual=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected))
 assert inv==json.loads(invbytes)
 verified[rp]=(receipt,inv,actual);return verified[rp]
def save():
 (out/'removal.json').write_text(json.dumps(dict(head=head,reports=reports,staging=staging_reports,skips=plan['skips'],scope='Only exact committed S3 archived single-link files removed after full remote member/body readback and fresh visible closure. Historical verdicts unchanged; logs and provenance retained. Protected live run and capacity donors untouched.'),indent=2)+'\n')
for item in plan['selected']:
 root=Path(item['root']);receipt,inventory,actual=remote(item['receipt'])
 before=fixture_archive.inventory(root)
 assert all(before.get(k)==v==inventory['files'].get(k) for k,v in item['files'].items())
 assert all((root/k).stat().st_nlink==1 for k in item['files'])
 observation=closure(root)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
 allocated=sum((root/k).stat().st_blocks*512 for k in item['files'])
 for k in item['files']:(root/k).unlink()
 for d in sorted((p for p in root.rglob('*') if p.is_dir()),key=lambda d:len(d.parts),reverse=True):
  if not any(d.iterdir()):d.rmdir()
 assert fixture_archive.inventory(root)=={k:v for k,v in before.items() if k not in item['files']}
 removed_path=out/(root.name+'-removed-files.json')
 removed_path.write_text(json.dumps(item['files'],indent=2)+'\n')
 reports.append(dict(root=str(root),receipt=item['receipt'],archive_url=receipt['archive']['url'],verified_remote=actual,closure=observation,allocated_bytes_removed=allocated,removed_files=len(item['files']),removed_manifest=str(removed_path.relative_to(repo))))
 save(); print('OFFLOADED',root.name,allocated,flush=True)
for item in json.loads(Path('/tmp/storage-staging-plan.json').read_text()):
 path=Path(item['path']);receipt,inventory,actual=remote(item['receipt'])
 assert actual==dict(bytes=item['bytes'],sha256=item['sha256'])
 with path.open('rb') as stream:assert s3.digest(stream)==actual
 observation=closure(path)
 assert path.is_file() and not path.is_symlink() and path.stat().st_nlink==1
 allocated=path.stat().st_blocks*512;path.unlink()
 staging_reports.append(dict(path=str(path),receipt=item['receipt'],verified_remote=actual,closure=observation,allocated_bytes_removed=allocated));save();print('REMOVED_STAGING',path.name,allocated,flush=True)
shutil.copyfile(__file__,out/'executed-removal.py')
print('TOTAL_REMOVED',sum(r['allocated_bytes_removed'] for r in reports+staging_reports),flush=True)
