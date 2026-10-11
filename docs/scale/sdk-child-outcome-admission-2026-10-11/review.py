import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
baseline=json.loads((out/'baseline-command.json').read_text())
assert baseline['actual_exit_code']==1
for name in ('context','select_many','outcome'):
 assert subprocess.check_output(['git','show',baseline['source_revision']+':wf/'+name+'.go'])==(out/('baseline-'+name+'.go.txt')).read_bytes()
assert json.loads((out/'baseline-overlay.json').read_text())['Replace']=={str(Path('wf/'+name+'.go').resolve()):str((out/('baseline-'+name+'.go.txt')).resolve()) for name in ('context','select_many','outcome')}
log=(out/'baseline.log').read_text()
assert log.count('--- FAIL: TestSDKChildOutcomeBeforeConsumption/')==150
assert log.count('--- PASS: TestSDKChildOutcomeBeforeConsumption/')==54
assert log.count('--- FAIL: TestSDKSelectedChildProvenanceBeforeConsumption/')==4
assert log.count('unverified child consumed or cached')==154
assert log.count('child provenance validator bypassed')==2
frame=json.loads((out/'frame-baseline-command.json').read_text())
assert frame['actual_exit_code']==1
assert subprocess.check_output(['git','show',frame['source_revision']+':internal/checkpoint/frame.go'])==(out/'baseline-frame.go.txt').read_bytes()
log=(out/'frame-baseline.log').read_text()
for marker in ('malformed promise frame admitted','malformed promise frame encoded','malformed promise restored SDK state'):
 assert log.count(marker)==2,marker
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(s['run'] for s in workflow['jobs']['terminal-outcome-admission']['steps'] if 'run' in s)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
seconds={};observed={}
for mode in ('normal','race','sdk','native'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit_code']==(1 if mode=='native' else 0) and receipt['end_ns']>receipt['start_ns']
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^\s*--- SKIP:',log,re.M)
 if mode!='native':assert not re.search(r'^\s*--- FAIL:',log,re.M)
 executables=json.loads((out/(mode+'-executables.json')).read_text())
 assert set(executables)==set(receipt['executables_captured'])
 expected={'normal':{'wf','worker'},'race':{'wf','worker','checkpoint'},'sdk':{'wf','checkpoint'},'native':{'worker'}}[mode]
 assert set(executables)==expected,(mode,set(executables))
 for name,exe in executables.items():
  assert exe['parent_pid']==receipt['driver_pid']
  assert receipt['start_ns']<=exe['captured_ns']<=receipt['end_ns']
  assert ('-race=true' in exe['build'])==(mode!='normal')
  assert f'/{name}.test ' in exe['command']
 if mode in ('normal','race'):
  exec(guard.replace("pathlib.Path('terminal-outcome-admission.log').read_text()",'log'),{'log':log})
 if mode=='sdk':
  assert '-run' not in receipt['command']
  for test in ('TestCheckpointStateMatchesUninterruptedContinuation','TestSDKChildOutcomeBeforeConsumption','TestSDKSelectedChildProvenanceBeforeConsumption','TestPromiseObjectReadRetryDoesNotConsumeAnotherSignal','TestPromiseRepeatedAwaitReplaysOneConsumptionAndCopiesResult'):
   assert f'--- PASS: {test} ' in log,test
 if mode=='native':
  for replicas in ('R1','R3Domain'):
   for archive in ('false','true'):
    for failed in ('false','true'):
     status='FAIL' if archive=='true' and failed=='false' else 'PASS'
     assert f'--- {status}: TestNativeGraphContinuationPromiseSelectionPriority/{replicas}/archive={archive}/failed={failed} ' in log
  for marker in ('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0','RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0','RAW_SDK_CHECKPOINT_HISTORY checkpoints=2'):
   assert log.count(marker)==6,marker
  assert log.count('SDK stage delivery journal append outcome unknown: context deadline exceeded <nil> <nil>')==2
  assert 'WARNING: DATA RACE' not in log
 seconds[mode]={pkg:float(elapsed) for pkg,elapsed in re.findall(r'ok\s+(js-wf/(?:internal/)?\w+)\s+([\d.]+)s',log)}
 observed[mode]=sorted(executables)
result={'regression_evidence_verified':True,'accepted':False,'native_compatibility_accepted':False,'source_inputs_unchanged':len(inputs),'sdk_child_metadata_controls':204,'positive_sdk_controls':54,'negative_sdk_controls':150,'sdk_provenance_denial_controls':6,'baseline_consumption_failures':154,'baseline_select_validator_bypasses':2,'failed_checkpoint_controls':2,'prior_metadata_result_export_checkpoint_controls':141,'total_named_ci_controls':353,'exact_current_terminal_ci_guards':True,'full_sdk_and_checkpoint_race_suite':True,'actual_final_executables_captured':sum(map(len,observed.values())),'captured_executables':observed,'normal_checkpoint_executable_missed':True,'native_promise_priority_attempts':8,'native_promise_priority_passes':6,'native_archived_successful_child_failures':2,'native_checkpoint_frames_verified':12,'native_failure_actual_exit':1,'native_failure_cause_confirmed':False,'test_seconds':seconds}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
