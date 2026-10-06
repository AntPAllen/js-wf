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
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
root=Path('/tmp/js-wf-journal-byte-bounded-explicit-routes-normal-2g-24h-20261005')
out=repo/'docs/scale/byte-bounded-audit-2026-10-05/complete-offload-proof-2026-10-06'
raw=Path('/tmp/js-wf-byte-bounded-journal-complete-offload-20261006.tar.gz')
execution=json.loads((root/'execution.json').read_text());assert execution['status']=='failed' and execution['test_exit_code']==1
review=json.loads(subprocess.check_output(['git','show',head+':docs/scale/byte-bounded-audit-2026-10-05/24h-failed-1750/actual-sdk.json'],cwd=repo))
pids=[execution['supervisor_pid'],execution['test_pid'],review['pid']]
for pid in pids:assert not Path('/proc',str(pid)).exists()
unit_name='js-wf-journal-byte-bounded-explicit-routes-normal-2g-24h-20261005.service'
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',unit_name,'-p','ActiveState','-p','MainPID','-p','LoadState'],text=True).splitlines())
assert unit['ActiveState'] in ('failed','inactive') and unit['MainPID']=='0'; assert unit['LoadState'] in ('loaded','not-found')
assert hashlib.sha256((root/'integration.test').read_bytes()).hexdigest()==review['sha256']==review['sha256']
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for name,sha in before['files'].items():assert hashlib.sha256((root/'source'/name).read_bytes()).hexdigest()==sha
ledger=json.loads((root/'archive-manifest.json').read_text())
for name,sha in ledger['files'].items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha
initial=closure(root);print('FAILED_SOURCE_BINARY_LEDGER_AND_CLOSURE_VERIFIED',len(ledger['files']),flush=True)
assert shutil.disk_usage(repo).free>1_200_000_000
proof=fixture_archive.capture(root,raw,out)
final=closure(root)
assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),archive=str(raw),archive_sha256=proof['archive_sha256'],archive_bytes=proof['archive_bytes'],members=proof['members'],source_revision=before['revision'],source_before_after_and_current_equal=True,existing_original_hash_ledger_equal=True,actual_sdk_sha256=review['sha256'],known_sdk_supervisor_pids_closed=pids,producer_unit_name=unit_name,producer_unit=unit,closure_before=initial,closure_after=final,native_verdict='failed',scope='Complete present original failed files captured and verified without reopening stores; preserves partial evidence and failed verdict. No qualification or adoption. No media removed before complete committed proof plus fresh S3 readback.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print('COMPLETE_CAPTURE_VERIFIED',proof,flush=True)
