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
root=Path('/tmp')/'js-wf-byte-refill-final-controls-20261005';canonical='docs/scale/byte-refill-diagnosis-2026-10-05/final-controls'
blob=lambda name:subprocess.check_output(['git','show',head+':'+canonical+'/'+name],cwd=repo)
actual_pids=[]
for mode in ('normal','race'):
 actual=json.loads((root/(mode+'-actual.json')).read_text());assert actual==json.loads(blob(mode+'-actual.json'))
 binary=json.loads((root/(mode+'-binary.json')).read_text());assert binary==json.loads(blob(mode+'-binary.json'))
 assert not Path('/proc',str(actual['pid'])).exists();actual_pids.append(actual['pid'])
 assert actual['exe']==str(root/('integration-'+mode+'.test'))
 assert hashlib.sha256(Path(actual['exe']).read_bytes()).hexdigest()==actual['exe_sha256']==binary['sha256']
 assert ('-race=true' in binary['build_info'])==(mode=='race')
 assert (root/(mode+'-test.log')).read_bytes()==blob(mode+'-test.log')
 assert (root/(mode+'-test.log')).read_text().rstrip().endswith('PASS')
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for name,h in before.items():assert hashlib.sha256((root/'selected-source'/name).read_bytes()).hexdigest()==h
ledger=json.loads((root/'archive-manifest.json').read_text())
for name,r in ledger.items():
 with (root/name).open('rb') as stream:assert s3.digest(stream)==r
initial=closure(root);print('CLOSED_REFILL_SOURCE_BINARIES_NATIVE_LOGS_LEDGER_VERIFIED',len(ledger),flush=True)
out=repo/'docs/scale/tmp-storage-review-2026-10-06/byte-refill-final-controls/complete';raw=Path('/tmp')/'js-wf-byte-refill-final-controls-complete-s3-20261006.tar.gz'
proof=fixture_archive.capture(root,raw,out);final=closure(root)
assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),archive=str(raw),complete_archive=proof,known_sdk_supervisor_pids_closed=actual_pids,source_before_after_current_equal=len(before),canonical_native_logs_and_observed_binary_records_equal=True,original_ledger_files_equal=len(ledger),producer_unit_name=None,producer_lifetime_scope='Recorded normal/race SDK PIDs absent and global visible root closure; no new historical unit/terminal lifetime observation claimed.',closure_before=initial,closure_after=final,scope='Complete current closed native byte-refill final controls preserved without fixtures reopened. Existing recorded working-tree/native source scope unchanged, including dirty-build provenance; no current-source/fullmatrix/24h qualification. No media/artifact removal until committed complete archive and fresh full S3 body/member/current census/closure checks.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print('REFILL_COMPLETE_CAPTURE_VERIFIED',proof,flush=True)
