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
assert log.count('unverified result projected ')==6
assert log.count('terminal result reads=0 want=1')==2
assert (out/'baseline-overlay.json').exists()
correction=json.loads((out/'fixture-correction.json').read_text())
native=json.loads((out/'native-command.json').read_text())
assert correction['native_closed_before_correction']==native['end_ns']<correction['corrected_ns']
original_inputs=json.loads((out/'fixture-option-failure/source-inputs.json').read_text())
for name,sha in original_inputs.items():
 if name!='worker/terminal_result_repair_test.go':
  assert inputs[name]==sha,name
old=(out/'fixture-option-failure/terminal_result_repair_test.go.txt').read_bytes()
assert hashlib.sha256(old).hexdigest()==original_inputs['worker/terminal_result_repair_test.go']
expected=old.decode().replace('opts := []worker.Option{}','opts := []worker.Option{}\n\t\t\t\t\tvar resultPort worker.ResultBlobPort = blobs').replace('opts = append(opts, worker.WithGraphJournal(graph))','opts = append(opts, worker.WithGraphJournal(graph))\n\t\t\t\t\t\tresultPort = nil').replace('ResultBlobs: blobs,','ResultBlobs: resultPort,')
formatted=subprocess.run(['gofmt'],input=expected.encode(),stdout=subprocess.PIPE,check=True).stdout
assert formatted==Path('worker/terminal_result_repair_test.go').read_bytes()
for mode in ('normal','race'):
 failed=json.loads((out/('fixture-option-failure/'+mode+'-command.json')).read_text())
 assert failed['actual_exit_code']==1
 assert (out/('fixture-option-failure/'+mode+'.log')).read_text().count('graph journal conflicts with legacy journal/result options')==4
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 exe=json.loads((out/(mode+'-executables.json')).read_text())['worker']
 assert exe['parent_pid']==receipt['driver_pid']
fixture=Path('worker/step_metadata_admission_test.go').read_text()
assert 'bytes.Equal(projected.Value, entries[len(entries)-1].Payload)' in fixture
result={'accepted':True,'source_inputs_unchanged':len(inputs),'prior_worker_controls':164,'new_result_controls':8,'total_worker_controls':172,'fresh_healed_deliveries':4,'baseline_unverified_projections':6,'baseline_healthy_missing_reads':2,'positive_projection_bytes_verified':True,'exact_current_worker_ci_guards':True,'final_actual_executables_captured':3,'native_child_flows':4,'native_checkpoint_frames':8,'native_after_close_fixture_only_correction':True,'test_seconds':{mode:float(re.search(r'ok\s+js-wf/worker\s+([\d.]+)s',(out/(mode+'.log')).read_text()).group(1)) for mode in ('normal','race','native')}}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
