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
root=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006');watch=Path('/tmp/js-wf-bulk-journal-24h-joined-watch-20261006')
original_unit='js-wf-bulk-journal-24h-joined-20261006.service';observer_unit='js-wf-watch-bulk-journal-24h-joined-20261006.service'
expected_source='757454ab9681f044af469a9462ecb7dfc63233ad';expected_sdk_pid=3461745
reviewer=Path('/tmp/js-wf-bulk-journal-24h-terminal-checker-pinned-20261006.py')
expected_reviewer_sha='25bb27cfbaad5e5eaaec8c6d64d5bd2b0b2f0387586e37b2c480d4664cf7f292'
assert hashlib.sha256(reviewer.read_bytes()).hexdigest()==expected_reviewer_sha
# Wait only on the two original handles. Native/observer timeouts and failures
# are preserved as separate facts; this never launches or restarts a campaign.
for name in (original_unit,observer_unit):
 while subprocess.check_output(['systemctl','--user','show',name,'-p','ActiveState','--value'],text=True).strip() in ('active','activating','deactivating'):
  print('WAITING_FOR_SAME_ORIGINAL_HANDLE',name,datetime.datetime.now(datetime.timezone.utc).isoformat(),flush=True);time.sleep(30)
units={name:dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',name,'-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines()) for name in (original_unit,observer_unit)}
assert all(x['MainPID']=='0' and x['ActiveState'] in ('inactive','failed') for x in units.values())
assert hashlib.sha256(reviewer.read_bytes()).hexdigest()==expected_reviewer_sha,'Reviewer changed: audit same native handles; no restart'
initial=closure(root);observer_closure=closure(watch)
out=repo/'docs/scale/bulk-journal-24h-2026-10-06/terminal';out.mkdir()
shutil.copyfile(__file__,out/'executed-wait-review.py');shutil.copyfile(reviewer,out/'executed-terminal-checker.py')
spec=importlib.util.spec_from_file_location('terminal',reviewer);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
try:
 result=module.review(root,watch,repo,'24h',expected_source,expected_sdk_pid)
 assert units[original_unit]['ExecMainStatus']=='0' and units[observer_unit]['ExecMainStatus']=='0'
 status='accepted_original24h_component'
except Exception as error:
 result=dict(accepted_component=False,qualifies_24h_component=False,clears_full_tier3_release=False,review_error=repr(error),scope='Original native/producer/observer/reviewer rejection preserved. Observation failure is not a native failure or permission to restart. No qualification from partial outputs.')
 status='review_rejected'
# Capture the actual SDK's final producer state, even if malformed/partial.
# Such bytes belong in preservation; they never become accepted evidence.
for filename in ('execution.json','source-before.json','source-after.json','binary.json'):
 if (root/filename).is_file():shutil.copy2(root/filename,out/filename)
for p in watch.iterdir():
 if p.is_file():shutil.copy2(p,root/('watch-'+p.name))
if (watch/'server-executables').exists():shutil.copytree(watch/'server-executables',root/'actual-server-executables')
result.update(review_status=status,original_units=units,expected_source=expected_source,expected_actual_sdk_pid=expected_sdk_pid,terminal_checker_sha256=expected_reviewer_sha,original_root_closure=initial,observer_root_closure=observer_closure,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(root/'independent-terminal-review.json').write_text(json.dumps(result,indent=2)+'\n');(out/'review.json').write_text(json.dumps(result,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-wait-review.py');shutil.copyfile(reviewer,root/'executed-terminal-checker.py')
final=closure(root)
proof=fixture_archive.capture(root,Path('/tmp/js-wf-bulk-journal-24h-joined-complete-20261006.tar.gz'),out,compresslevel=1)
(out/'preservation.json').write_text(json.dumps(dict(status=status,proof=proof,closure_before=initial,closure_before_capture=final,closure_after_capture=closure(root),stores_reopened=False,scope='Complete current closed original root plus observer records preserved. Commit canonical metadata and full S3 readback remain separate; no original files removed.'),indent=2)+'\n')
print('ORIGINAL24H_TERMINAL_REVIEW_AND_PRESERVATION',status,flush=True)
