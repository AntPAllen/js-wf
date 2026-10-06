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


root=Path('/tmp/js-wf-raft-safety170-20261006')
out=Path(str(root)+'-proof')
assert not Path('/proc/1664461').exists() and not Path('/proc/1685193').exists()
initial=closure(root)
proof=fixture_archive.capture(root,Path(str(root)+'.tar.gz'),out,compresslevel=1)
final=closure(root)
log=Path('/tmp/full-nrg-producer-20261006.log').read_text()
report=dict(original_producer_exit_code=1,original_collector_failure='live fixture process',collector_failure_context='A cleanup pipeline launcher shell contained the fixture path in its command text during final closure. The original collector rejected it; native test failures preceded this secondary collection failure. This is a fresh storage capture after actual test and producer termination, not a repaired native qualification.',native_results={name:json.loads((root/(name+'-execution.json')).read_text())['result'] for name in ['upstream','candidate']},closure_before=initial,closure_after=final,original_producer_log=log,proof=proof,scope='Storage preservation only. Original exit codes and failures retained; no tests rerun or native gate promoted.')
(out/'terminal-storage-capture.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-terminal-storage-capture.py')
print(json.dumps(dict(proof=proof,native_results=report['native_results']),indent=2),flush=True)
