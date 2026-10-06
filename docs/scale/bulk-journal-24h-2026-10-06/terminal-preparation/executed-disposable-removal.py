from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os,time
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
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
root=Path('/tmp/js-wf-bulk-soak-terminal-review-control-20261006')
base=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-journal-ten-minute'
manifest=json.loads((base/'fixture-inventory.json').read_bytes());meta=json.loads((base/'archive-verification.json').read_bytes())
assert fixture_archive.inventory(root/'restored')==manifest['files']
with (root/'proof.tar.gz').open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert json.loads((root/'current-control-state.json').read_text())['restored_census_unchanged']
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-control-bulk-soak-terminal-review-20261006.service','-p','ActiveState','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='inactive',ExecMainStatus='0')
initial=closure(root)
paths=[root/'restored',root/'observer',root/'proof.tar.gz'];allocated=sum(p.stat().st_blocks*512 for parent in paths for p in (parent.rglob('*') if parent.is_dir() else [parent]) if p.is_file())
for p in paths:
 if p.is_dir():shutil.rmtree(p)
 else:p.unlink()
out=repo/'docs/scale/bulk-journal-24h-2026-10-06/terminal-preparation'
shutil.copyfile(__file__,out/'executed-disposable-removal.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),removed_paths=[str(p) for p in paths],allocated_disposable_bytes_removed=allocated,root_visible_closure=initial,exact_restored_census_verified=True,raw_archive_fingerprint_verified=True,original_fixture_reopened=False,scope='Fresh restored file/control staging only, not recovered older data. Original S3 archive and canonical proofs unchanged. Control results/commands/logs remain. No SDK or broker was started.')
(out/'disposable-removal.json').write_text(json.dumps(report,indent=2)+'\n');print('DISPOSABLE_CONTROL_FILES_REMOVED',allocated,flush=True)
