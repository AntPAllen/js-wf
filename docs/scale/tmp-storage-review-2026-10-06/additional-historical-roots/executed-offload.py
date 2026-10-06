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
base=repo/'docs/scale/tmp-storage-review-2026-10-06/additional-historical-roots'
reports=[]
for out in sorted(p for p in base.iterdir() if p.is_dir()):
 root=Path('/tmp')/out.name
 if (out/'offload.json').exists():continue
 rel=out.relative_to(repo);meta=json.loads(blob(rel/'archive-verification.json'));manifest=json.loads(blob(rel/'fixture-inventory.json'));receipt=json.loads(blob(rel/'s3-readback.json'))
 for file,key in [('archive-verification.json','metadata'),('fixture-inventory.json','inventory')]:
  data=blob(rel/file);assert get(receipt[key]['url'],s3.digest)==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 inv,actual=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected));assert inv==manifest
 before=fixture_archive.inventory(root);assert before==manifest['files']
 # Bulk nested evidence/data and large files are recoverable from complete S3 copies.
 names={k:v for k,v in before.items() if (root/k).stat().st_nlink==1 and (('/' in k and k.split('/')[0] not in ('reviewer','model-source')) or v['bytes']>=1048576)}
 assert names
 observation=closure(root)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
 allocated=sum((root/k).stat().st_blocks*512 for k in names)
 for k in names:(root/k).unlink()
 for d in sorted((p for p in root.rglob('*') if p.is_dir()),key=lambda d:len(d.parts),reverse=True):
  if not any(d.iterdir()):d.rmdir()
 assert fixture_archive.inventory(root)=={k:v for k,v in before.items() if k not in names}
 raw=Path('/tmp')/(out.name+'-storage-second-complete-20261006.tar.gz')
 with raw.open('rb') as stream:assert s3.digest(stream)==expected
 rawclosure=closure(raw);staging=raw.stat().st_blocks*512;assert raw.stat().st_nlink==1;raw.unlink()
 (out/'removed-files.json').write_text(json.dumps(names,indent=2)+'\n')
 report=dict(head=head,root=str(root),receipt=str(rel/'s3-readback.json'),archive_url=receipt['archive']['url'],verified_remote=actual,closure=observation,staging_closure=rawclosure,allocated_bytes_removed=allocated,staging_bytes_removed=staging,removed_files=len(names),scope='Historical closed nested evidence/source/media and large files only; exact archived inventory verified; root logs/provenance and small reviewer/model source retained. No native verdict changed or broker opened.')
 (out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');reports.append(report);print('OFFLOADED_NEW',out.name,allocated,staging,flush=True)
shutil.copyfile(__file__,base/'executed-offload.py')
print('TOTAL_NEW',sum(r['allocated_bytes_removed'] for r in reports),flush=True)
