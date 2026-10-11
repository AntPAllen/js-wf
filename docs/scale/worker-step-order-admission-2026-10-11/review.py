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
assert log.count('handler admission: handlers=1 want=0 effects=0')==8
assert log.count('corrupt delivery projected an outcome: keys=[test.metadata] err=<nil>')==2
for backend in ('legacy','canonical'):
 for case in ('balanced','suspended_between_pair','pending_request','balanced_terminal','failed_pending'):
  assert f'--- PASS: TestWorkerStepOrderBeforeHandler/{backend}/{case} ' in log
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 executable=json.loads((out/(mode+'-executables.json')).read_text())['worker']
 assert executable['parent_pid']==receipt['driver_pid']
 failed=json.loads((out/('empty-kv-assertion-'+mode+'-command.json')).read_text())
 assert failed['actual_exit_code']==1
 assert 'keys=[] err=nats: no keys found' in (out/('empty-kv-assertion-'+mode+'.log')).read_text()
helper='worker/step_metadata_admission_test.go'
old=(out/'native-step-metadata-test.go.txt').read_bytes()
update=json.loads((out/'test-helper-update.json').read_text())
assert hashlib.sha256(old).hexdigest()==update['before_sha256']
assert hashlib.sha256(Path(helper).read_bytes()).hexdigest()==update['after_sha256']
assert update['observed_ns']>json.loads((out/'native-command.json').read_text())['end_ns']
expected=old.decode().replace('"encoding/json"','"encoding/json"\n "errors"').replace('"time"','"time"\n\n "github.com/nats-io/nats.go/jetstream"').replace('if e != nil || len(keys) != 0 {','if e != nil && !errors.Is(e, jetstream.ErrNoKeysFound) || len(keys) != 0 {')
assert subprocess.check_output(['gofmt'],input=expected.encode())==Path(helper).read_bytes()
native_inputs=json.loads((out/'native-source-inputs.json').read_text())
assert set(native_inputs)==set(inputs)
for name,sha in native_inputs.items():
 data=old if name==helper else Path(name).read_bytes()
 assert hashlib.sha256(data).hexdigest()==sha,name
result={'accepted':True,'source_inputs_unchanged':len(inputs),'step_order_controls':20,'positive_step_order_controls':10,'negative_step_order_controls':10,'epoch_owner_controls':22,'metadata_controls':60,'baseline_invalid_handler_admissions':8,'baseline_invalid_terminal_projections':2,'test_helper_only_changed_after_native_closure':True,'exact_current_worker_ci_guards':True,'final_actual_executables_captured':3,'native_child_flows':4,'native_checkpoint_frames':8,'test_seconds':{mode:float(re.search(r'ok\s+js-wf/worker\s+([\d.]+)s',(out/(mode+'.log')).read_text()).group(1)) for mode in ('normal','race','native')}}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
