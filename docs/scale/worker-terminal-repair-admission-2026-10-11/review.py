import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(s['run'] for s in workflow['jobs']['worker-step-metadata']['steps'] if 'run' in s)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
for mode in ('race','normal','native'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit_code']==0 and receipt['end_ns']>receipt['start_ns']
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^--- (FAIL|SKIP):',log,re.M)
 if mode=='native':
  executable=json.loads((out/'native-executable.json').read_text())
  assert 'TestNativeGraphContinuation' in executable['command']
  for marker in ('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0','RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0','RAW_SDK_CHECKPOINT_HISTORY checkpoints=2'):
   assert log.count(marker)==4,marker
 else:
  executable=json.loads((out/(mode+'-executables.json')).read_text())['worker']
  assert 'TestWorkerStepMetadataBeforeHandler' in executable['command']
  exec(guard.replace("pathlib.Path('worker-step-metadata.log').read_text()",'log'),{'log':log})
 assert receipt['start_ns']<=executable['captured_ns']<=receipt['end_ns']
 assert ('-race=true' in executable['build'])==(mode!='normal')
baseline=json.loads((out/'baseline-command.json').read_text())
assert baseline['actual_exit_code']==1
assert subprocess.check_output(['git','show',baseline['source_revision']+':worker/worker.go'])==(out/'baseline-worker.go.txt').read_bytes()
log=(out/'baseline.log').read_text()
assert log.count('corrupt delivery projected an outcome: keys=[test.metadata] err=<nil>')==48
for backend in ('legacy','canonical'):
 for case in ('inline','user_null','opaque_user_duplicates','explicit_empty_error','valid_failed','valid_failed_opaque_request','valid_empty_result'):
  assert f'--- PASS: TestWorkerTerminalOutcomeBeforeRepair/{backend}/{case} ' in log
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 exe=json.loads((out/(mode+'-executables.json')).read_text())['worker']
 assert exe['parent_pid']==receipt['driver_pid']
fixture=Path('worker/step_metadata_admission_test.go').read_text()
assert 'bytes.Equal(projected.Value, entries[len(entries)-1].Payload)' in fixture
result={'accepted':True,'source_inputs_unchanged':len(inputs),'terminal_repair_controls':62,'positive_terminal_controls':14,'negative_terminal_controls':48,'prior_worker_controls':102,'total_worker_controls':164,'baseline_invalid_terminal_projections':48,'positive_projection_bytes_verified':True,'exact_current_worker_ci_guards':True,'final_actual_executables_captured':3,'native_child_flows':4,'native_checkpoint_frames':8,'test_seconds':{mode:float(re.search(r'ok\s+js-wf/worker\s+([\d.]+)s',(out/(mode+'.log')).read_text()).group(1)) for mode in ('normal','race','native')}}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
