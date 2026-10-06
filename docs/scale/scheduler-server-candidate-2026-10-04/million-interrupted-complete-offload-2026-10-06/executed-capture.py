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
root=Path('/tmp/js-wf-native-million-scheduler-candidate-24h-20261004')
actual=json.loads((root/'actual-program.json').read_text());native=json.loads((root/'actual-native-processes.json').read_text())
launch=json.loads((root/'launch.json').read_text())
reference=json.loads(subprocess.check_output(['git','cat-file','blob',head+':docs/scale/scheduler-server-candidate-2026-10-04/million-24h-launch/review.json'],cwd=repo))
assert actual['pid']==launch['pid']==215956 and launch['revision']=='247eeff78e9aaae8d9d12be7214c38f67c3e9175'
assert hashlib.sha256((root/'wf-timer-volume').read_bytes()).hexdigest()==actual['sha256']==launch['actual_program_sha256']=='659920c096170bbfba8fef858af8f683117d3f31af92168199e4dd0434d22550'
server=hashlib.sha256((root/'campaign/nats-server-candidate').read_bytes()).hexdigest();assert server==launch['actual_candidate_sha256']=='ff7335643d02125ec50b8abaa0ca80fe4da36473aa14402c2dbcf029e63c9fe8'
assert reference['source']==launch['revision'] and reference['actual_program_pid']==actual['pid'] and reference['actual_program_sha256']==actual['sha256'] and reference['candidate_sha256']==server
assert reference['actual_native_pids']==[r['pid'] for r in native]
for row in native:assert row['sha256']==server and row['executable_link']==str(root/'campaign/nats-server-candidate')
pids=[215701,actual['pid']]+[r['pid'] for r in native]
for pid in pids:assert not Path('/proc',str(pid)).exists()
unit_name='js-wf-native-million-scheduler-candidate-24h-20261004.service'
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',unit_name,'-p','ActiveState','-p','MainPID','-p','LoadState'],text=True).splitlines());assert unit['MainPID']=='0' and unit['ActiveState'] in ('inactive','failed')
source=json.loads((root/'source-before.json').read_text());paths=json.loads((root/'captured-paths.json').read_text());assert set(source)==set(paths)
for name,h in source.items():assert hashlib.sha256((root/paths[name]).read_bytes()).hexdigest()==h
ledger=json.loads((root/'archive-manifest.json').read_text())
for name,record in ledger.items():
 with (root/name).open('rb') as stream:assert s3.digest(stream)==record
observation=json.loads((root/'interruption-observation/observation.json').read_text());assert 'no terminal result or physical drain' in observation['qualification']
initial=closure(root);print('INTERRUPTED_SDK_CANDIDATE_SOURCE_LEDGER_CLOSURE_VERIFIED',len(ledger),flush=True)
out=repo/'docs/scale/scheduler-server-candidate-2026-10-04/million-interrupted-complete-offload-2026-10-06';raw=Path('/tmp/js-wf-native-million-interrupted-complete-offload-20261006.tar.gz')
proof=fixture_archive.capture(root,raw,out);final=closure(root)
assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),archive=str(raw),archive_sha256=proof['archive_sha256'],archive_bytes=proof['archive_bytes'],members=proof['members'],source_revision=launch['revision'],captured_compile_inputs_verified=len(source),existing_size_hash_ledger_verified=len(ledger),actual_sdk_sha256=actual['sha256'],actual_unadopted_candidate_sha256=server,known_sdk_supervisor_pids_closed=pids,producer_unit_name=unit_name,producer_unit=unit,native_verdict='interrupted; no observed terminal SDK result or physical drain',preserved_stale_report_status=json.loads((root/'campaign/report.json').read_text())['status'],closure_before=initial,closure_after=final,scope='Complete interrupted original/current/partial files captured without stores reopened. Missing historical unit and absent known SDK/server/supervisor are closure evidence, not an SDK exit-code or qualification. Stale running report retained unchanged; candidate unadopted, million/24h unqualified. No media removal before committed proof and complete remote verification.')
(out/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print('COMPLETE_INTERRUPTED_MILLION_CAPTURE_VERIFIED',proof,flush=True)
