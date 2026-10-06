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
root=Path('/tmp/js-wf-r5-large-resumption-20261005');canonical='docs/scale/bulk-read-resumption-2026-10-05/r5-100k'
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
execution=json.loads((root/'execution.json').read_text());assert execution==json.loads(blob('execution.json')) and execution['exit_code']==0
review=json.loads(blob('review.json'));assert review['exit_code']==0
assert not Path('/proc',str(execution['pid'])).exists()
assert hashlib.sha256((root/'integrity.test').read_bytes()).hexdigest()==execution['sha256']==review['actual_test_executable_sha256']
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for name,h in before.items():assert hashlib.sha256((root/'source'/name).read_bytes()).hexdigest()==h
ledger=json.loads((root/'archive-manifest.json').read_text())
for name,record in ledger.items():
 with (root/name).open('rb') as stream:assert s3.digest(stream)==record
assert review['invocations']==100000 and review['journal_entries']==review['visited']==1200000
assert review['baseline_seconds']<20 and review['interrupted_seconds']<20
for name,h in review['actual_container_binary_hashes'].items():
 assert hashlib.sha256((root/'actual-containers'/name).read_bytes()).hexdigest()==h
initial=closure(root);print('NATIVE_BINARY_SOURCE_LEDGER_CLOSURE_VERIFIED',len(ledger),flush=True)
out=repo/'docs/scale/bulk-read-resumption-2026-10-05/r5-100k-complete-offload-2026-10-06';raw=Path('/tmp/js-wf-r5-large-resumption-complete-offload-20261006.tar.gz')
proof=fixture_archive.capture(root,raw,out);final=closure(root)
assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),archive=str(raw),archive_sha256=proof['archive_sha256'],archive_bytes=proof['archive_bytes'],members=proof['members'],source_revision=execution['base_commit'],source_before_after_current_equal=True,existing_original_hash_size_ledger_equal=True,actual_sdk_sha256=execution['sha256'],known_sdk_supervisor_pids_closed=[execution['pid']],producer_unit_name=None,producer_lifetime_scope='Manual producer: exact recorded SDK absent plus global visible root process/descriptor/mount closure; no unit exit observation claimed.',closure_before=initial,closure_after=final,native_verdict='passed',scope='Complete present recorded R5 100k/1.2M explicit injected pull suffix-loss original captured without stores reopened; all overlay/test/source/log evidence retained. Existing executed-source pass scope unchanged; no current-source, natural server-cause, OS-kill or full/24h qualification. No media removal before committed proof and full S3 readback.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print('COMPLETE_R5_100K_RESUMPTION_CAPTURE_VERIFIED',proof,flush=True)
