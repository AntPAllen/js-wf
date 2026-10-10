"""Independently qualify terminal durable Start input binding evidence."""
import datetime,hashlib,io,json,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'state.json').read_text());root,checkout=Path(state['root']),Path(state['checkout'])
assert state.get('phase')=='closed' and state['exit']==0 and state['finished']
properties=subprocess.check_output(['systemctl','--user','show','js-wf-replay-input-binding-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','InvocationID','-p','LoadState'],text=True)
assert 'MainPID=0\n' in properties
assert 'LoadState=loaded\n' in properties
assert 'ExecMainStatus=0\n' in properties and 'InvocationID='+state['invocation']+'\n' in properties
loaded=True
supervisor_exit=0
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
assert len(state['commands'])==11 and [r['exit'] for r in state['commands']]==[0,0,0,0,0,0,0,0,1,0,0]
assert all(r['finished'] for r in state['commands'])
assert not any(a.startswith('-test.run=') for a in state['commands'][1]['command'])
assert '-test.run=^TestPinnedRegressionCorpus$' in state['commands'][3]['command']
assert '-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin|TestReplayDeclaredGraphBeforePlugin|TestReplayInputBindingBeforePlugin|TestNativeGraphCursorOperatorHistory)$' in state['commands'][5]['command']
assert set(state['binaries'])=={'wf-race.test','sim-normal.test','cli-race.test','worker-race.test','wf','handler.so'}
for name,artifact in state['binaries'].items():
 path=root/name;assert path.stat().st_size==artifact['bytes'] and hash(path)==artifact['sha256']
 assert ('-race=true' in subprocess.check_output(['go','version','-m',str(path)],text=True))==(name!='sim-normal.test')
def events(name):
 rows=[json.loads(line) for line in (root/name).read_text().splitlines()]
 assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows)
 assert len([r for r in rows if r['Action']=='pass' and 'Test' not in r])==1
 return rows
wf=events('wf-events.jsonl');sim=events('sim-events.jsonl');cli=events('cmd-wf-events.jsonl');worker=events('worker-events.jsonl')
passed={r['Test'] for r in wf if r['Action']=='pass' and 'Test' in r}
inventory=subprocess.check_output([str(root/'wf-race.test'),'-test.list=.'],cwd=checkout/'wf',text=True).splitlines()
tops={n for n in passed if '/' not in n};assert tops=={n for n in inventory if n.startswith('Test')}
assert len(tops)==65
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
formats={'valid','unversioned_compatibility','unknown_version','missing_annotation','null_annotation','wrong_queue_index','missing_token','missing_object','changed_object','inline_signal','zero_sequence','alias_annotation','checkpoint_metadata_removed','limit_nested_annotation_removed'}
assert {n.split('/',1)[1] for n in passed if n.startswith('TestReplayDeclaredGraphFormat/')}==formats
cp={r['Test'] for r in cli if r['Action']=='pass' and 'Test' in r}
assert {n.split('/',1)[1] for n in cp if n.startswith('TestReplayDeclaredGraphBeforePlugin/')}=={'removed_annotation','unknown_format','removed_checkpoint_metadata'}
assert {n.split('/',1)[1] for n in cp if n.startswith('TestNativeGraphCursorOperatorHistory/')}=={'v'+str(v)+'/R'+str(r)+'-domain' for v in (4,5,6) for r in (1,3)}
wp={r['Test'] for r in worker if r['Action']=='pass' and 'Test' in r}
assert {n.split('/',1)[1] for n in wp if n.startswith('TestGraphReplaySnapshotOwnsInputsAndRejectedSignals/')}=={'consumed','rejected','unreadable','forged-source','release-unknown','forged-terminal'}
assert {n.split('/',1)[1] for n in wp if n.startswith('TestGraphReplaySnapshotRangeAndSlowAcquisition/')}=={'census','slow-acquisition'}
production=checkout/'wf/replay_graph_format.go'
needle='func validateReplayInputBinding(records []journal.Record, format, inputHash string) error {'
assert json.loads((root/'overlay.json').read_text())=={'Replace':{str(production):str(root/'validator-disabled.go')}}
assert (root/'validator-disabled.go').read_text()==production.read_text().replace(needle,needle+'\n return nil // required negative control')
negative=(root/'validator-disabled.log').read_text()
actual={line.strip().split()[2] for line in negative.splitlines() if line.startswith('    --- FAIL:')}
assert actual=={'TestReplayInputBindsStarted/graph_rehashed_input','TestReplayInputBindsStarted/unversioned_annotated_changed'}
input_modes={'graph_valid','graph_rehashed_input','graph_missing_expected','graph_missing_start_hash','graph_malformed_hash','graph_duplicate_key','graph_case_alias','graph_unknown_field','unversioned_annotated_valid','unversioned_annotated_changed','legacy_without_declaration','legacy_without_expected'}
assert {n.split('/',1)[1] for n in passed if n.startswith('TestReplayInputBindsStarted/')}==input_modes
assert {n.split('/',1)[1] for n in cp if n.startswith('TestReplayInputBindingBeforePlugin/')}=={'format=','format=graph-v1'}
producer=base.parent/'graph-continuation-child-offline-2026-10-10'
assert hash(producer/'review.json')==state['producer_review_sha256']
pr=json.loads((producer/'review.json').read_text());assert pr['accepted'] and pr['source']==state['producer_source'] and pr['git_verified_inputs']==3312
assert len(state['replays'])==209
labels={}
for row in state['replays']:
 path=Path(row['input']);assert hash(path)==row['input_sha256']
 args=row['command'];assert args[1:3]==['-url','nats://127.0.0.1:1'] and args[8]==str(path) and args[-1]=='replay'
 if row['label'].startswith('input-old-'):
  old=json.loads((base/'diagnostic/report.json').read_text())
  assert args[0]==old['cli'] and args[4]==old['plugin']
 else:assert args[0]==str(root/'wf') and args[4]==str(root/'handler.so')
 labels[row['label']]=labels.get(row['label'],0)+1
 bundle=json.loads(path.read_text())
 if '-native-' in row['label']:
  original=Path(row['native_input']);assert hash(original)==row['native_input_sha256']
  assert hash(original)==hash(producer/'qualified/artifacts'/original.relative_to('/home/exedev/js-wf-child-offline-20261010/artifacts'))
  old=json.loads(original.read_text())
  if row['label'].startswith('graph-v1-'):
   assert bundle.pop('format')=='graph-v1' and bundle==old
  else:assert bundle==old and 'format' not in bundle
 if row['expected'] in ('completed','suspended'):
  report=json.loads(row['output']);assert row['exit']==0 and report['status']==row['expected']
  if row['expected']=='completed':assert report['result']==60
  elif row['label'].endswith('native-suspended.json'):assert report['waiting_on']=='signal:gate'
  else:assert report['waiting_on'].startswith('signal:child_')
 else:assert row['exit']!=0 and row['expected'] in row['output']
for mode in ('graph-v1','unversioned'):
 assert labels[mode+'-native-completed.json']==20 and labels[mode+'-native-suspended.json']==20 and labels[mode+'-native-child-pending.json']==8
 assert labels[mode+'-native-missing-objects.json']==20 and labels[mode+'-native-missing-child-result.json']==8 and labels[mode+'-native-missing-stage']==20
for name in ('control','missing-canonical','all-markers-removed','checkpoint-metadata-removed','wrong-queue-index','missing-token'):
 assert labels['variant-unversioned-'+name]==labels['variant-graph-'+name]==1
 original=json.loads((root/'variants'/(name+'-unversioned.json')).read_text())
 declared=json.loads((root/'variants'/(name+'-graph.json')).read_text())
 assert declared.pop('format')=='graph-v1' and declared==original
assert labels['variant-graph-unknown-format']==1
# Bind diagnostic transformations to the actual qualified producer's control.
control=json.loads((producer/'qualified/artifacts/offline-2667111011/completed.json').read_text())
import copy
for name in ('control','missing-canonical','all-markers-removed','checkpoint-metadata-removed','wrong-queue-index','missing-token'):
 expected=copy.deepcopy(control)
 for r in expected['journal']:
  e=r.get('payload')
  if not isinstance(e,dict):continue
  if r['kind']=='SignalConsumed':
   if name in ('missing-canonical','all-markers-removed'):e.pop('canonical_signal',None)
   if name=='all-markers-removed':e.pop('graph_child',None)
   if name=='wrong-queue-index':e['canonical_signal']['index']+=1
   if name=='missing-token':e['canonical_signal'].pop('token',None)
  if name in ('all-markers-removed','checkpoint-metadata-removed'):
   e.pop('checkpoint_metadata_ref',None);e.pop('checkpoint_metadata_hash',None)
 assert json.loads((root/'variants'/(name+'-unversioned.json')).read_text())==expected
unknown=copy.deepcopy(control);unknown['format']='graph-v99'
assert json.loads((root/'variants/unknown-format-graph.json').read_text())==unknown
assert not (root/'effect').exists()
diagnostic=json.loads((base/'diagnostic/report.json').read_text())
assert diagnostic['source']=='04f542bcf8ec9099feb07e509db97361c504a633'
for key in ('cli','plugin'):assert hash(Path(diagnostic[key]))==diagnostic[key+'_sha256']
for r in diagnostic['rows']:
 assert labels['input-fixed-'+r['name']]==labels['input-old-'+r['name']]==1
 for row in state['replays']:
  if row['label'] in ('input-fixed-'+r['name'],'input-old-'+r['name']):
   assert row['input_sha256']==r['input_sha256']
   assert json.loads(Path(row['input']).read_text())==json.loads((base/'diagnostic'/(r['name']+'.json')).read_text())
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(names),full_sdk_race_top_tests=len(tops),input_binding_sdk_controls=12,input_binding_preplugin_controls=2,format_controls=14,metadata_controls=62,selected_child_controls=10,cli_format_preplugin_controls=3,native_export_cases=6,worker_export_controls=8,normal_saved_traces=841,cli_provenance_controls=20,legacy_plugin_pass=True,required_negative_failures=2,offline_cli_invocations=209,healthy_native_replays_each_format=48,missing_objects_each_format=28,missing_stages_each_format=20,declared_graph_mutation_rejections=6,unversioned_compatibility_controls=6,old_changed_input_accepted=True,new_changed_input_rejected=True,actual_supervisor_exit=0,supervisor_exit_proven=True,producer_source=state['producer_source'],reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Replay input binding to canonical Start declaration, export/library/CLI controls and retained native bundles. External authenticity, full import/fault/retention/admission/latest seeded/extended and broader original gates remain open.')
result['events_sha256']={n:hash(root/n) for n in ('wf-events.jsonl','sim-events.jsonl','cmd-wf-events.jsonl','worker-events.jsonl')}
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
