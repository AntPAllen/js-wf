"""Independently qualify terminal metadata validation evidence only."""
import datetime,hashlib,io,json,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'state.json').read_text());root,checkout=Path(state['root']),Path(state['checkout'])
assert state.get('phase')=='closed' and state['exit']==0 and state['finished']
properties=subprocess.check_output(['systemctl','--user','show','js-wf-checkpoint-metadata-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','InvocationID','-p','LoadState'],text=True)
assert 'MainPID=0\n' in properties
# An unloaded transient unit exposes default success/zero values. Those are not
# an actual exit receipt. Individual completed child command receipts remain.
loaded='LoadState=loaded\n' in properties
if loaded:
 assert 'ExecMainStatus=0\n' in properties and 'InvocationID='+state['invocation']+'\n' in properties
else:
 assert 'LoadState=not-found\n' in properties
supervisor_exit=0 if loaded else None
(root/'supervisor-exit.txt').write_text(properties)
def hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()
before,after=[json.loads((root/n).read_text()) for n in ('source-before.json','source-after.json')]
assert before==after and before['source']==state['source']
tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=checkout,text=True).splitlines()
names=[n for n in tracked if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(names)==set(before['files'])
raw=subprocess.check_output(['git','cat-file','--batch'],cwd=checkout,input=''.join(state['source']+':'+n+'\n' for n in names).encode());stream=io.BytesIO(raw)
for name in names:
 header=stream.readline().decode().split();assert header[1]=='blob'
 body=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(body).hexdigest()==before['files'][name]==hash(checkout/name)
assert len(state['commands'])==9 and [r['exit'] for r in state['commands']]==[0,0,0,0,0,0,1,0,0]
assert all(r['finished'] for r in state['commands'])
assert not any(a.startswith('-test.run=') for a in state['commands'][1]['command'])
assert '-test.run=^TestPinnedRegressionCorpus$' in state['commands'][3]['command']
assert '-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin)$' in state['commands'][5]['command']
assert set(state['binaries'])=={'wf-race.test','sim-normal.test','cli-race.test','wf','handler.so'}
for name,artifact in state['binaries'].items():
 path=root/name;assert path.stat().st_size==artifact['bytes'] and hash(path)==artifact['sha256']
 assert ('-race=true' in subprocess.check_output(['go','version','-m',str(path)],text=True))==(name!='sim-normal.test')
def events(name):
 rows=[json.loads(line) for line in (root/name).read_text().splitlines()]
 assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows)
 assert len([r for r in rows if r['Action']=='pass' and 'Test' not in r])==1
 return rows
wf=events('wf-events.jsonl');sim=events('sim-events.jsonl');cli=events('cmd-wf-events.jsonl')
passed={r['Test'] for r in wf if r['Action']=='pass' and 'Test' in r}
inventory=subprocess.check_output([str(root/'wf-race.test'),'-test.list=.'],cwd=checkout/'wf',text=True).splitlines()
tops={n for n in passed if '/' not in n};assert tops=={n for n in inventory if n.startswith('Test')}
assert len(tops)==63
modes={'valid','legacy','missing_metadata','changed_bytes','missing_hash','missing_reference','wrong_hash','null_reference','wrong_version','wrong_type','wrong_id','wrong_invocation','wrong_anchor','wrong_epoch','wrong_frame_hash','wrong_signal_next','wrong_signal_last','missing_child','foreign_child','resolved_child_declaration','extra_child','extra_signal','duplicate_version','alias_version','unknown_field','non_checkpoint','missing_frame','changed_frame'}
expected={'TestReplayGraphCheckpointMetadata/buffered='+b+'/'+m for b in ('false','true') for m in modes}
expected|={'TestReplayGraphCheckpointMetadata/buffered=true/'+m for m in ('missing_buffered','changed_payload','changed_token','changed_binding','wrong_sequence','wrong_child')}
assert len(expected)==62 and {n for n in passed if n.startswith('TestReplayGraphCheckpointMetadata/')}==expected
child={'TestReplayGraphChildSelectedSignal/async='+a+'/'+b for a in ('false','true') for b in ('legacy','unbound','bound')}
child|={'TestReplayGraphChildSelectedAfterCheckpoint/legacy='+a+'/child='+b for a in ('false','true') for b in ('false','true')}
assert child<=passed
corpus=sorted(Path(n).name for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
assert len(corpus)==841
assert sorted(r['Test'].split('/',1)[1] for r in sim if r['Action']=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/'))==corpus
assert any(r['Action']=='pass' and r.get('Test')=='TestPinnedRegressionCorpus' for r in sim)
controls={'valid','wrong_type','wrong_id','wrong_invocation','zero_invocation','wrong_reference','wrong_hash','missing_binding','missing_outcome','missing_result','changed_outcome_bytes','changed_result_bytes','annotation_before_request','undeclared_name','legacy_signal','ordinary_before_child_request','failed_child','failed_with_result','inline_result','limit_nested_signal'}
assert {r['Test'].split('/',1)[1] for r in cli if r['Action']=='pass' and r.get('Test','').startswith('TestReplayGraphChildProvenance/')}==controls
assert any(r['Action']=='pass' and r.get('Test')=='TestReplayContinuationPlugin' for r in cli)
production=checkout/'wf/replay_graph_checkpoint.go'
needle='func ValidateReplayGraphCheckpoints(records []journal.Record, objects map[string][]byte, typ, id string, invocation uint64) error {'
assert json.loads((root/'overlay.json').read_text())=={'Replace':{str(production):str(root/'validator-disabled.go')}}
assert (root/'validator-disabled.go').read_text()==production.read_text().replace(needle,needle+'\n return nil // required negative control')
negative=(root/'validator-disabled.log').read_text()
actual={line.strip().split()[2] for line in negative.splitlines() if line.startswith('    --- FAIL:')}
assert actual=={'TestReplayGraphCheckpointMetadata/buffered=false/missing_metadata','TestReplayGraphCheckpointMetadata/buffered=false/wrong_anchor'}
producer=base.parent/'graph-continuation-child-offline-2026-10-10'
assert hash(producer/'review.json')==state['producer_review_sha256']
pr=json.loads((producer/'review.json').read_text());assert pr['accepted'] and pr['source']==state['producer_source'] and pr['git_verified_inputs']==3312
old_bytes=subprocess.check_output(['git','show',state['source']+':docs/scale/graph-offline-checkpoint-metadata-2026-10-10/old-diagnostic.json'],cwd=checkout)
assert old_bytes==(base/'old-diagnostic.json').read_bytes()
old=json.loads(old_bytes)
for r in old['rows']:
 fixture=subprocess.check_output(['git','show',state['source']+':docs/scale/graph-offline-checkpoint-metadata-2026-10-10/diagnostic/'+r['name']+'.json'],cwd=checkout)
 assert hashlib.sha256(fixture).hexdigest()==r['input_sha256']
assert old['reader_source']=='02c3506b76b326237c00987f6f31994f833fe0b9'
for key in ('cli','plugin'):assert hash(Path(old[key]))==old[key+'_sha256']
assert len(state['replays'])==110
labels={}
for row in state['replays']:
 path=Path(row['input']);assert hash(path)==row['input_sha256']
 args=row['command'];assert args[1:3]==['-url','nats://127.0.0.1:1'] and args[8]==str(path) and args[-1]=='replay'
 if row['label'].startswith('old-'):assert args[0]==old['cli'] and args[4]==old['plugin']
 else:assert args[0]==str(root/'wf') and args[4]==str(root/'handler.so')
 labels[row['label']]=labels.get(row['label'],0)+1
 if row['label'].startswith('native-'):
  assert hash(path)==hash(producer/'qualified/artifacts'/path.relative_to('/home/exedev/js-wf-child-offline-20261010/artifacts'))
 else:
  name=row['label'].split('-',1)[1]
  assert row['input_sha256']==next(r['input_sha256'] for r in old['rows'] if r['name']==name)
 if row['expected'] in ('completed','suspended'):
  report=json.loads(row['output']);assert row['exit']==0 and report['status']==row['expected']
  if row['expected']=='completed':assert report['result']==60
  elif row['label']=='native-suspended.json':assert report['waiting_on']=='signal:gate'
  else:assert report['waiting_on'].startswith('signal:child_')
 else:assert row['exit']!=0 and row['expected'] in row['output']
assert labels['native-completed.json']==20 and labels['native-suspended.json']==20 and labels['native-child-pending.json']==8
assert labels['native-missing-objects.json']==20 and labels['native-missing-child-result.json']==8 and labels['native-missing-stage']==20
for r in old['rows']:assert labels['fixed-'+r['name']]==labels['old-'+r['name']]==1
assert not (root/'effect').exists()
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(names),full_sdk_race_top_tests=len(tops),metadata_directed_controls=62,existing_child_selection_controls=10,required_negative_failures=2,normal_saved_traces=841,cli_provenance_controls=20,legacy_plugin_pass=True,offline_cli_invocations=110,healthy_native_replays=48,native_missing_objects=28,native_missing_stages=20,malformed_metadata_new_rejections=6,malformed_metadata_old_acceptances=6,actual_supervisor_exit=supervisor_exit,supervisor_exit_proven=loaded,producer_source=state['producer_source'],reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Individual source-qualified child test/CLI results; the original unloaded supervisor exit is unproven. Annotated exported checkpoint metadata validation; SDK race, normal saved corpus and retained native replay controls only. Full missing-marker/import/native faults/retention/admission, latest seeded/extended and broader original gates remain open.')
result['events_sha256']={n:hash(root/n) for n in ('wf-events.jsonl','sim-events.jsonl','cmd-wf-events.jsonl')}
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
