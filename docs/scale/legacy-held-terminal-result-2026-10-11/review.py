import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(s['run'] for s in workflow['jobs']['terminal-outcome-admission']['steps'] if 'run' in s)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
observed=0;seconds={}
for mode in ('normal','race','native'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit_code']==0 and receipt['end_ns']>receipt['start_ns']
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
 executables=json.loads((out/(mode+'-executables.json')).read_text())
 for name,exe in executables.items():
  assert exe['parent_pid']==receipt['driver_pid']
  assert receipt['start_ns']<=exe['captured_ns']<=receipt['end_ns']
  assert ('-race=true' in exe['build'])==(mode!='normal')
  assert f'/{name}.test ' in exe['command']
  observed+=1
 if mode=='native':
  assert set(executables)=={'integration'}
  for timer in ('false','true'):
   for external in ('false','true'):
    assert f'--- PASS: TestTerminalWakeupsDrainWhileHealthyLeaseHeld/timer={timer}/external={external} ' in log
  assert '--- PASS: TestLargeTerminalResultSpillsAndAwaitVerifies ' in log
  assert log.count('terminal held wakeups=32 drain=')==4
  assert log.count('unchanged')==4
 else:
  assert set(executables)==({'wf'} if mode=='normal' else {'wf','worker'})
  exec(guard.replace("pathlib.Path('terminal-outcome-admission.log').read_text()",'log'),{'log':log})
 seconds[mode]={pkg:float(elapsed) for pkg,elapsed in re.findall(r'ok\s+(js-wf/\w+)\s+([\d.]+)s',log)}
baseline=json.loads((out/'baseline-command.json').read_text())
assert baseline['actual_exit_code']==1
assert subprocess.check_output(['git','show',baseline['source_revision']+':worker/terminal_delivery.go'])==(out/'baseline-terminal-delivery.go.txt').read_bytes()
log=(out/'baseline.log').read_text()
assert log.count('held result handlers=0 acks=1 want=0 naks=0 want=1')==7
assert log.count('held result reads=0 want=1')==1
assert '--- PASS: TestLegacyHeldTerminalResultBeforeAck/failed_empty ' in log
original=json.loads((out/'initial-stop-condition/baseline-command.json').read_text())
assert original['actual_exit_code']==1 and original['end_ns']<baseline['start_ns']
old=(out/'initial-stop-condition/held_terminal_result_test.go.txt').read_bytes()
assert old.replace(b'transport.Dispatch.StopWhenDrained(cancel)',b'transport.Dispatch.StopAfterNextAck(cancel)')==Path('worker/held_terminal_result_test.go').read_bytes()
result={'accepted':True,'source_inputs_unchanged':len(inputs),'new_held_result_controls':9,'positive_controls':2,'negative_controls':7,'fresh_healed_deliveries':2,'prior_metadata_controls':63,'total_metadata_and_result_controls':72,'baseline_invalid_acks':7,'baseline_healthy_missing_reads':1,'exact_current_terminal_ci_guards':True,'actual_executables_captured':observed,'normal_worker_executable_missed':True,'native_held_controls':4,'native_duplicate_acks':128,'native_external_result_duplicate_acks':64,'native_large_terminal_corruption_control':True,'test_seconds':seconds}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
