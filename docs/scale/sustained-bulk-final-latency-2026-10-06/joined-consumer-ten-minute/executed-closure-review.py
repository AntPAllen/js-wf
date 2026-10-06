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
root=Path('/tmp/js-wf-bulk-consumer-ten-minute-joined-20261006')
out=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-consumer-ten-minute'
for role in ('','watch-','review-'):
 name='js-wf-'+role+'bulk-consumer-ten-minute-joined-20261006.service'
 unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',name,'-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
 assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0'),(name,unit)
meta=json.loads((out/'archive-verification.json').read_text());manifest=json.loads((out/'fixture-inventory.json').read_text())
assert fixture_archive.inventory(root)==manifest['files']
raw=Path('/tmp/js-wf-bulk-consumer-ten-minute-joined-complete-20261006.tar.gz')
with raw.open('rb') as stream:actual,compressed=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert actual==manifest
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_three_units_terminal_status0=True,current_complete_census_equals_canonical=True,complete_compressed_archive_and_every_member_equal=compressed,closure=closure(root),scope='Supplemental complete current census/compressed archive and global visible root process/descriptor/mount closure after independent accepted original10m consumer row review. Visibility limits retained; no old stores opened or broader promotion.')
(out/'closure-supplement.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-closure-review.py');print('COMPLETE_CONSUMER_PROOF_AND_GLOBAL_VISIBLE_CLOSURE_VERIFIED',flush=True)
