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

names=['js-wf-tier2-current-consumer25-36-37149506857', 'js-wf-tier2-current-consumer193-200-37149506857', 'js-wf-hosted-partition-seed3-download-20261005']

head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base=repo/'docs/scale/tmp-storage-review-2026-10-06/consumer-partition-evidence'
config=s3.credentials()
def get(url,verify):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try: result=verify(p.stdout)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 return result
for name in names:
 root=Path('/tmp')/name;out=base/name
 if (out/'offload.json').exists():continue
 canonical=str(out.relative_to(repo))
 blob=lambda file:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+file],cwd=repo)
 meta=json.loads(blob('archive-verification.json'));manifest=json.loads(blob('fixture-inventory.json'));receipt=json.loads(blob('s3-readback.json'))
 for file,key in [('archive-verification.json','metadata'),('fixture-inventory.json','inventory')]:
  data=blob(file);assert get(receipt[key]['url'],s3.digest)==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 remote,actual=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected));assert remote==manifest
 assert fixture_archive.inventory(root)==manifest['files']
 observation=closure(root)
 paths=[root/'raw'] if name != names[-1] else [root/'tier2-retained-partition-3-37347246576-1'/'originals']
 paths=[p for p in paths if p.exists()]
 removed={k:v for k,v in manifest['files'].items() if any(root/k==p or p.is_dir() and (root/k).is_relative_to(p) for p in paths)}
 shared_files_retained=[k for k in removed if (root/k).stat().st_nlink!=1]
 removed={k:v for k,v in removed.items() if k not in shared_files_retained}
 assert removed
 for k in removed:assert (root/k).stat().st_nlink==1
 allocated=sum((root/k).stat().st_blocks*512 for k in removed)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
 for k in removed:(root/k).unlink()
 for p in paths:
  if p.is_dir():
   for directory in sorted((d for d in p.rglob('*') if d.is_dir()),key=lambda d:len(d.parts),reverse=True):
    if not any(directory.iterdir()):directory.rmdir()
   if not any(p.iterdir()):p.rmdir()
 remaining={k:v for k,v in manifest['files'].items() if k not in removed}
 assert fixture_archive.inventory(root)==remaining
 report=dict(head=head,root=str(root),archive_url=receipt['archive']['url'],full_remote_body_and_members_verified=actual,all_current_bytes_modes_mtimes_verified_before_removal=True,closure=observation,offloaded_paths=[str(p) for p in paths],offloaded_files=len(removed),shared_files_retained=shared_files_retained,allocated_older_bytes_recovered=allocated,remaining_files=len(remaining),scope='Historical closed matrix raw evidence and store media only; local source/log/metadata retained. Existing success/failure reports remain unchanged. No original reopened; future audits require fresh verified S3 restore.')
 raw=Path('/tmp')/(name+'-storage-offload-complete-20261006.tar.gz')
 with raw.open('rb') as f:assert s3.digest(f)==expected
 closed.verify_no_open_originals(raw);report['staging_bytes_removed']=raw.stat().st_blocks*512;raw.unlink()
 (out/'offload.json').write_text(json.dumps(report,indent=2)+'\n')
 print('OFFLOADED',name,allocated,flush=True)
shutil.copyfile(__file__,base/'executed-offload.py')
