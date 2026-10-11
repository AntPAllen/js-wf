import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
native_inputs=json.loads((out/'native-source-inputs.json').read_text())
old_test=(out/'native-graph-terminal-test.go.txt').read_bytes()
old_ci=(out/'native-ci.yml.txt').read_bytes()
assert old_test.decode().replace('journal.EncodingJSON','journal.JSON').replace('journal.EncodingProtobuf','journal.ProtobufV1')==Path('wf/graph_terminal_metadata_test.go').read_text()
assert old_ci.decode().replace("for codec in ('json', 'protobuf'):","for codec in ('json', 'protobuf-v1'):")==Path('.github/workflows/graph-publication.yml').read_text()
for name,sha in native_inputs.items():
 data=old_test if name=='wf/graph_terminal_metadata_test.go' else old_ci if name=='.github/workflows/graph-publication.yml' else Path(name).read_bytes()
 assert hashlib.sha256(data).hexdigest()==sha,name
correction=json.loads((out/'test-ci-correction.json').read_text())
assert correction['observed_ns']>json.loads((out/'native-command.json').read_text())['end_ns']
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(s['run'] for s in workflow['jobs']['terminal-outcome-admission']['steps'] if 'run' in s)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text());assert receipt['actual_exit_code']==0
 log=(out/(mode+'.log')).read_text()
 exec(guard.replace("pathlib.Path('terminal-outcome-admission.log').read_text()",'log'),{'log':log})
 exes=json.loads((out/(mode+'-executables.json')).read_text())
 assert set(exes)==({'wf','worker'} if mode=='race' else {'wf'})
 for e in exes.values():
  assert e['parent_pid']==receipt['driver_pid']
  assert receipt['start_ns']<=e['captured_ns']<=receipt['end_ns']
  assert ('-race=true' in e['build'])==(mode=='race')
receipt=json.loads((out/'native-command.json').read_text());assert receipt['actual_exit_code']==0
native=(out/'native.log').read_text()
for marker in ('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0','RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0','RAW_SDK_CHECKPOINT_HISTORY checkpoints=2'):assert native.count(marker)==4
assert not re.search(r'^--- (FAIL|SKIP):',native,re.M)
e=json.loads((out/'native-executable.json').read_text());assert '-race=true' in e['build'] and receipt['start_ns']<=e['captured_ns']<=receipt['end_ns']
baseline=json.loads((out/'baseline-command.json').read_text());assert baseline['actual_exit_code']==1
mapping=json.loads((out/'baseline-overlay.json').read_text())['Replace']
assert set(mapping)=={str(Path(n).resolve()) for n in ('wf/graph_outcome.go','worker/terminal_delivery.go')}
for n in ('wf/graph_outcome.go','worker/terminal_delivery.go'):
 assert Path(mapping[str(Path(n).resolve())]).read_bytes()==subprocess.check_output(['git','show',baseline['source_revision']+':'+n])
log=(out/'baseline.log').read_text()
assert 'build failed' not in log and log.count('malformed terminal admitted')==34
assert log.count('held terminal: handlers=0 acks=1 want=0 naks=0 want=1')==17
for codec in ('json','protobuf-v1'):
 for name in ('inline','user_null','opaque_user_duplicates','explicit_empty_error'):assert f'--- PASS: TestReadGraphTerminalMetadataAdmission/{codec}/{name} ' in log
for name in ('inline','user_null','opaque_user_duplicates','explicit_empty_error'):assert f'--- PASS: TestLegacyHeldTerminalMetadataAdmission/{name} ' in log
result={'accepted':True,'final_source_inputs_unchanged':len(inputs),'production_unchanged_after_native_closure':True,'terminal_controls':63,'positive_controls':12,'negative_controls':51,'baseline_bad_graph_terminals_accepted':34,'baseline_bad_held_acks':17,'focused_baseline_two_file_overlay':True,'exact_current_ci_guards':True,'final_executables_observed':4,'normal_worker_executable_missed':True,'native_child_flows':4,'native_checkpoint_frames':8}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
