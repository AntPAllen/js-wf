import sys,json,subprocess,time,datetime,os,hashlib,shutil
from pathlib import Path
sys.dont_write_bytecode=True
root=Path('/tmp/js-wf-corrected-partition200-20261008');record=json.loads((root/'preparation.json').read_text());source=Path(record['source_checkout']);gate=record['resource_gate'];sdk=gate['sdk']
shutil.copy2(__file__,root/'executed-resource-gate.py')
state=dict(source=record['source'],supervisor_pid=os.getpid(),supervisor_start_ticks=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19],started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),status='waiting_for_original_race_handle',native_started=False,resource_gate=gate,command=record['command'])
def save():
 pending=root/'resource-gate.json.new';pending.write_text(json.dumps(state,indent=2)+'\n');pending.replace(root/'resource-gate.json')
def unit_state():
 return dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',gate['unit'],'--property=LoadState,MainPID,ExecMainPID,InvocationID,ActiveState,SubState,Result,ExecMainStatus'],text=True).splitlines())
save()
try:
 while True:
  unit=unit_state();assert unit['LoadState']=='loaded' and unit['InvocationID']==gate['invocation_id'],'Original race supervisor identity changed; no native campaign started'
  proc=Path('/proc',str(sdk['pid']));live=False
  try:
   birth=(proc/'stat').read_text().rsplit(') ',1)[1].split()[19]
   live=birth==sdk['start_ticks']
   if live:
    assert (proc/'exe').resolve()==Path(sdk['exe'])
    assert (proc/'cmdline').read_bytes().split(b'\0')[:-1]==[s.encode() for s in sdk['args']]
  except (FileNotFoundError,ProcessLookupError):pass
  state.update(last_observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),original_sdk_live=live,original_unit=unit);save()
  if not live and unit['MainPID']=='0' and unit['SubState'] in ('exited','failed','dead'):
   state.update(status='original_race_handles_closed',race_closed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save();break
  time.sleep(5)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==record['source']
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 program=Path(record['command'][record['command'].index('--partition-server')+1])
 with program.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==record['candidate_sha256']
 assert shutil.disk_usage('/tmp').free>=20*1024**3
 state.update(status='starting_fresh_campaign',native_started=None);save()
 with (root/'campaign-producer.log').open('w') as log:
  child=subprocess.Popen(record['command'],cwd=source,stdout=log,stderr=subprocess.STDOUT)
  state.update(status='campaign_producer_active',campaign_producer_pid=child.pid);save()
  sys.path.insert(0,str(source/'scripts'));from live_process_admission import snapshot
  while child.poll() is None:
   execution=root/'campaign/seed-001/execution.json'
   if not state.get('native_started') and execution.exists():
    try:
     native=json.loads(execution.read_text());pid=native['pid'];profile=native['environment']
     first=snapshot(pid,profile)
     with Path('/proc',str(pid),'exe').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
     second=snapshot(pid,profile)
     keys=['pid','start_ticks','args','exe','working_directory','environment']
     assert {k:first[k] for k in keys}=={k:second[k] for k in keys} and digest==native['sha256']
     assert second['start_ticks']==native['start_ticks'] and second['args']==native['actual_argv'] and native['source']==record['source']
     state.update(native_started=True,first_sdk_admission=dict(actual=second,exe_sha256=digest,source=native['source'],stable_identity_observed_twice=True));save()
    except (FileNotFoundError,ProcessLookupError,KeyError,json.JSONDecodeError):pass
   time.sleep(.25)
  code=child.wait()
 state.update(status='campaign_producer_passed' if code==0 else 'campaign_producer_failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 sys.exit(code)
except BaseException as error:
 state.update(status='supervisor_failed',error=repr(error),finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
