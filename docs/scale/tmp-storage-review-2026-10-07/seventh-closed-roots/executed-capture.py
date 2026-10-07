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


names=json.loads(Path('/tmp/storage-seventh-names.json').read_text())
base=Path('/tmp/storage-seventh-proofs')
for name in names:
 root=Path('/tmp')/name; out=base/name; raw=Path('/tmp')/(name+'-storage-seventh-complete-20261007.tar.gz')
 if (out/'capture.json').exists():continue
 if any(p.is_symlink() for p in root.rglob('*')):
  print('SKIPPED_SYMLINK',name,flush=True)
  if out.exists() and not any(out.iterdir()):out.rmdir()
  continue
 try: initial=closure(root)
 except AssertionError as error:
  print("SKIPPED_ACTIVE",name,str(error),flush=True);continue
 print('CLOSED',name,flush=True)
 if (out/'archive-verification.json').exists():
  proof=json.loads((out/'archive-verification.json').read_text());manifest=json.loads((out/'fixture-inventory.json').read_text())
  with raw.open('rb') as stream: assert fixture_archive.verify_hashed_stream(stream,dict(bytes=proof['archive_bytes'],sha256=proof['archive_sha256']))[0]==manifest
  assert fixture_archive.inventory(root)==manifest['files']
 else:proof=fixture_archive.capture(root,raw,out,compresslevel=1)
 final=closure(root)
 report=dict(root=str(root),archive=str(raw),closure_before=initial,closure_after=final,complete_archive=proof,scope='Storage preservation of the complete current closed root. Historical verdict and source scope unchanged; binaries previously offloaded remain recoverable from their separate index. No native test or broker opened.')
 (out/'capture.json').write_text(json.dumps(report,indent=2)+'\n')
 print('CAPTURED',name,proof['archive_bytes'],flush=True)
shutil.copyfile(__file__,base/'executed-capture.py')
