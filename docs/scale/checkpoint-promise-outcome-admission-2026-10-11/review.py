import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
baseline=json.loads((out/'baseline-command.json').read_text())
assert baseline['actual_exit_code']==1
assert subprocess.check_output(['git','show',baseline['source_revision']+':internal/checkpoint/frame.go'])==(out/'baseline-frame.go.txt').read_bytes()
log=(out/'baseline.log').read_text()
for marker in ('malformed promise frame admitted','malformed promise frame encoded','malformed promise restored SDK state'):
 assert log.count(marker)==17,marker
positive=('inline','user_null','opaque_user_duplicates','explicit_empty_error','legacy_identity_omitted','valid_failed','opaque_limit_request','opaque_limit_payload','external')
for case in positive+('unknown_field',):
 assert f'--- PASS: TestCheckpointPromiseOutcomeMetadataAdmission/{case} ' in log
initial=json.loads((out/'initial-state-read-assertion/baseline-command.json').read_text())
assert initial['actual_exit_code']==1 and initial['end_ns']<baseline['start_ns']
assert (out/'initial-state-read-assertion/baseline.log').read_text().count('restore appended records 2')==9
compile=json.loads((out/'initial-helper-compile/normal-command.json').read_text())
assert compile['actual_exit_code']==1 and compile['executables_captured']==[]
assert 'internal/checkpoint/outcome.go:13:5: undefined: checkpoint' in (out/'initial-helper-compile/normal.log').read_text()
assert (out/'initial-helper-compile/outcome.go.txt').read_bytes().replace(b'checkpoint.DecodeUnambiguous',b'DecodeUnambiguous')==Path('internal/checkpoint/outcome.go').read_bytes()
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(s['run'] for s in workflow['jobs']['terminal-outcome-admission']['steps'] if 'run' in s)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
seconds={};observed={}
for mode in ('normal','race','sdk','native'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit_code']==0 and receipt['end_ns']>receipt['start_ns']
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
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
  assert '--- PASS: TestCheckpointStateMatchesUninterruptedContinuation ' in log
  assert '--- PASS: TestCheckpointPromiseOutcomeMetadataAdmission ' in log
  assert '--- PASS: TestFrameCanonicalEnvelopePreservesOpaqueLocals ' in log
 if mode=='native':
  for marker in ('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0','RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0','RAW_SDK_CHECKPOINT_HISTORY checkpoints=2'):
   assert log.count(marker)==4,marker
 seconds[mode]={pkg:float(elapsed) for pkg,elapsed in re.findall(r'ok\s+(js-wf/(?:internal/)?\w+)\s+([\d.]+)s',log)}
 observed[mode]=sorted(executables)
result={'accepted':True,'source_inputs_unchanged':len(inputs),'checkpoint_promise_controls':27,'positive_controls':9,'negative_controls':18,'baseline_bad_frame_decode_encode_sdk_restore':17,'prior_metadata_result_export_controls':114,'total_metadata_result_export_checkpoint_controls':141,'exact_current_terminal_ci_guards':True,'full_sdk_and_checkpoint_race_suite':True,'actual_final_executables_captured':sum(map(len,observed.values())),'captured_executables':observed,'normal_checkpoint_executable_missed':True,'native_child_flows':4,'native_checkpoint_frames':8,'test_seconds':seconds}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
