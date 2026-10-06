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

root=Path('/tmp/js-wf-timer-volume-million-service-20261001')
archive=Path('/tmp/js-wf-million-primary-complete-20261006.tar.gz')
out=Path('/tmp/js-wf-million-primary-complete-20261006-proof')
initial=closure(root)
proof=fixture_archive.capture(root,archive,out,compresslevel=1)
final=closure(root)
source_report=json.loads((root/'report.json').read_text())
assert source_report['status']=='failed' and source_report['revision']=='92586ea3ff579362b5ab61e7026b6d1f244a2d90' and source_report['unique_received']==1000000
report=dict(root=str(root),archive=str(archive),closure_before=initial,closure_after=final,complete_archive=proof,recorded_original_revision=source_report['revision'],scope='Complete current original failed million-timer service root including all three physical stores, receipt ledger, observations and report. Storage preservation only; no broker opened, no drain zero/default field or original million gate promoted. Native executable/provenance remain in separate original92586ea evidence.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-capture.py')
print(json.dumps(dict(members=proof['members'],bytes=proof['archive_bytes'],sha256=proof['archive_sha256'])),flush=True)
