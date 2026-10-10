"""Independently review terminal native CLI replay evidence; never infer exit."""
import datetime, hashlib, io, json, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
state = json.loads((base / 'state.json').read_text())
root, checkout = Path(state['root']), Path(state['checkout'])
assert state['exit'] == 0 and state['finished']
assert len(state['commands']) == 10
assert [c['exit'] for c in state['commands']] == [0,0,0,1,0,0,0,0,0,0]
assert state['commands'][1]['command'] == ['go','test','-race','-c','-o',str(root/'wf-race.test'),'./wf']
assert state['commands'][2]['cwd'] == str(checkout/'wf')
assert not any(a.startswith('-test.run=') for a in state['commands'][2]['command'])
assert state['commands'][4]['command'] == ['go','test','-c','-o',str(root/'sim-normal.test'),'./sim']
assert '-test.run=^TestSavedSeedReplayCorpus$' in state['commands'][5]['command']
assert state['commands'][6]['command'] == ['go','test','-race','-c','-o',str(root/'cli-race.test'),'./cmd/wf']
assert '-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin)$' in state['commands'][7]['command']
assert state['commands'][8]['command'] == ['go','build','-race','-o',str(root/'wf'),'./cmd/wf']
assert state['commands'][9]['command'] == ['go','build','-race','-buildmode=plugin','-o',str(root/'handler.so'),'./worker/testdata/graphreplayplugin']
properties=subprocess.check_output(['systemctl','--user','show','js-wf-selected-child-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','ActiveState','-p','InvocationID','-p','LoadState'],text=True)
assert 'MainPID=0\n' in properties
loaded='LoadState=loaded\n' in properties
if loaded:
 assert 'ExecMainStatus=0\n' in properties and 'InvocationID='+state['invocation']+'\n' in properties
else:
 assert 'LoadState=not-found\n' in properties
supervisor_exit=0 if loaded else None
(root/'supervisor-exit.txt').write_text(properties)
before, after = [json.loads((root / name).read_text()) for name in ['source-before.json', 'source-after.json']]
assert before == after and before['source'] == state['source']
names = list(before['files'])
tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=checkout,text=True).splitlines()
assert set(names)=={n for n in tracked if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')}
assert set(state['binaries'])=={'wf-race.test','sim-normal.test','cli-race.test','wf','handler.so'}
raw = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=checkout, input=''.join(state['source'] + ':' + n + '\n' for n in names).encode())
stream = io.BytesIO(raw)
for name in names:
    header = stream.readline().decode().split()
    assert header[1] == 'blob'
    body = stream.read(int(header[2]))
    assert stream.read(1) == b'\n'
    assert hashlib.sha256(body).hexdigest() == before['files'][name]
    assert hashlib.sha256((checkout / name).read_bytes()).hexdigest() == before['files'][name]
for name, artifact in state['binaries'].items():
    path=root/name
    assert path.stat().st_size==artifact['bytes']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==artifact['sha256']
    info=subprocess.check_output(['go','version','-m',str(path)],text=True)
    assert ('-race=true' in info) == (name != 'sim-normal.test')
rows = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
assert not any(r['Action'] in ('fail', 'skip') for r in rows)

expected = {'valid','wrong_type','wrong_id','wrong_invocation','zero_invocation','wrong_reference','wrong_hash','missing_binding','missing_outcome','missing_result','changed_outcome_bytes','changed_result_bytes','annotation_before_request','undeclared_name','legacy_signal','ordinary_before_child_request','failed_child','failed_with_result','inline_result','limit_nested_signal'}
prefix='TestReplayGraphChildProvenance/'
assert {r['Test'][len(prefix):] for r in rows if r['Action']=='pass' and r.get('Test','').startswith(prefix)}==expected
for top in ('TestReplayGraphChildProvenance','TestReplayContinuationPlugin'):
    assert any(r['Action']=='pass' and r.get('Test')==top for r in rows)
assert any(r['Action']=='pass' and 'Test' not in r for r in rows)
assert len(state['replays'])==106
labels={}
for row in state['replays']:
    path=Path(row['input'])
    assert hashlib.sha256(path.read_bytes()).hexdigest()==row['input_sha256']
    args=row['command']
    assert args[1:3]==['-url','nats://127.0.0.1:1'] and args[-1]=='replay'
    assert args[8]==str(path)
    if not row['label'].startswith('diagnostic-old-'):
        assert args[0]==str(root/'wf') and args[4]==str(root/'handler.so')
    labels[row['label']]=labels.get(row['label'],0)+1
    if row['expected'] in ('completed','suspended'):
        assert row['exit']==0
        output=json.loads(row['output'])
        assert output['status']==row['expected']
        if row['expected']=='completed': assert output['result']==60
        elif row['label']=='native-suspended.json': assert output['waiting_on']=='signal:gate'
        elif row['label']=='native-child-pending.json': assert output['waiting_on'].startswith('signal:child_')
    else:
        assert row['exit']!=0 and row['expected'] in row['output']
assert labels['native-completed.json']==20 and labels['native-suspended.json']==20
assert labels['native-child-pending.json']==8 and labels['native-missing-objects.json']==20
assert labels['native-missing-child-result.json']==8 and labels['native-missing-stage']==20
for case in ('control','wrong-child-type','wrong-child-id','wrong-child-invocation','wrong-child-result-ref'):
    assert labels['diagnostic-fixed-'+case]==labels['diagnostic-old-'+case]==1
assert not (root/'effect').exists()
native_base=base.parent/'graph-continuation-child-offline-2026-10-10'
native_review=json.loads((native_base/'review.json').read_text())
assert native_review['accepted'] and native_review['source']==state['native_source']
assert native_review['git_verified_inputs']==3312
correction=json.loads((native_base/'review-count-correction.json').read_text())
assert correction['review_exit']==0 and correction['corrected_summary_count']==3312
# The producer's original verifier checked all inputs but its summary count
# shadowed a variable. Bind the actual original checker and corrected receipt.
original=(native_base/'review.py').read_bytes()
assert hashlib.sha256(original).hexdigest()==state['native_review_sha256']
old=json.loads((base.parent/'graph-offline-child-provenance-2026-10-10/diagnostic/report.json').read_text())
for key in ('cli','plugin'):
    assert hashlib.sha256(Path(old[key]).read_bytes()).hexdigest()==old[key+'_sha256']
# Verify the complete SDK inventory and all ten selected-signal controls.
wrows=[json.loads(line) for line in (root/'wf-events.jsonl').read_text().splitlines()]
assert not any(r['Action'] in ('fail','skip') for r in wrows)
inventory=subprocess.check_output([str(root/'wf-race.test'),'-test.list=.'],cwd=checkout/'wf',text=True).splitlines()
tops={r['Test'] for r in wrows if r['Action']=='pass' and '/' not in r.get('Test','') and 'Test' in r}
assert tops == {n for n in inventory if n.startswith('Test')}
controls={'TestReplayGraphChildSelectedSignal/async='+a+'/'+b for a in ('false','true') for b in ('legacy','unbound','bound')}
controls |= {'TestReplayGraphChildSelectedAfterCheckpoint/legacy='+a+'/child='+b for a in ('false','true') for b in ('false','true')}
assert controls <= {r.get('Test') for r in wrows if r['Action']=='pass'}
assert any(r['Action']=='pass' and 'Test' not in r for r in wrows)
overlay=json.loads((root/'overlay.json').read_text())
assert overlay=={'Replace':{str(checkout/'wf/replay.go'):str(root/'selector-disabled.go')}}
original=(checkout/'wf/replay.go').read_text()
assert original.count('c.SetChildResultValidator(childValidator)')==1
assert (root/'selector-disabled.go').read_text()==original.replace('c.SetChildResultValidator(childValidator)','c.SetChildResultValidator(nil)\n        _ = childValidator')
negative=(root/'selector-disabled.log').read_text()
expected_negative={'TestReplayGraphChildSelectedSignal/async=false/unbound','TestReplayGraphChildSelectedSignal/async=true/unbound','TestReplayGraphChildSelectedAfterCheckpoint/legacy=false/child=true'}
actual_negative={line.strip().split()[2] for line in negative.splitlines() if line.startswith('    --- FAIL:')}
assert actual_negative==expected_negative and negative.count('result=42 err=<nil>')==3
# The initial selector is invalid evidence: explicitly require zero test cases.
wrong=[json.loads(line) for line in (root/'corpus-events.jsonl').read_text().splitlines()]
assert not any('Test' in r for r in wrong)
assert any('no tests to run' in r.get('Output','') for r in wrong)
supplement=json.loads((base/'corpus-correction.json').read_text())
assert supplement['source']==state['source'] and len(supplement['commands'])==2
assert all(c['exit']==0 for c in supplement['commands'])
assert '-test.run=^TestPinnedRegressionCorpus$' in supplement['commands'][1]['command']
assert supplement['binary_sha256']==state['binaries']['sim-normal.test']['sha256']
assert (root/'corpus-inventory.txt').read_text().splitlines()==['TestPinnedRegressionCorpus']
corpus=[json.loads(line) for line in (root/'corpus-corrected-events.jsonl').read_text().splitlines()]
prefix='TestPinnedRegressionCorpus/'
actual=sorted(r['Test'][len(prefix):] for r in corpus if r['Action']=='pass' and r.get('Test','').startswith(prefix))
expected=sorted(Path(n).name for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
assert len(expected)==841 and actual==expected
assert not any(r['Action'] in ('fail','skip') for r in corpus)
assert any(r['Action']=='pass' and r.get('Test')=='TestPinnedRegressionCorpus' for r in corpus)
assert any(r['Action']=='pass' and 'Test' not in r for r in corpus)
supplement_review=json.loads((base/'corpus-correction-review.json').read_text())
assert supplement_review['accepted'] and supplement_review['actual_supervisor_exit']==1
assert supplement_review['passed_cases']==841 and supplement_review['expected_cases']==expected
assert supplement_review['actual_corpus_exit']==0 and supplement_review['binary_sha256']==supplement['binary_sha256']
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(before['files']),native_source=state['native_source'],native_git_verified_inputs=3312,full_sdk_top_tests=len(tops),selected_signal_controls=10,required_negative_failures=3,directed_cli_controls=20,legacy_continuation_plugin_pass=True,offline_cli_runs=106,positive_native_replays=48,negative_native_objects=28,negative_native_stages=20,malformed_new_rejections=4,malformed_old_acceptances=4,corpus_passed=841,initial_corpus_selector_valid=False,supplemental_corpus_go_exit=0,supplemental_corpus_supervisor_exit=1,supervisor_exit=supervisor_exit,supervisor_exit_proven=loaded,reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Individual source-qualified child command results; original unloaded supervisor exit unproven. Shared SDK child selection and structural provenance; full SDK race suite, corrected source-bound corpus, retained healthy native bundles. Full latest-source seeded/fault/import/rollout and broader original requirements remain open.')
result['evidence_sha256']={n:hashlib.sha256((root/n).read_bytes()).hexdigest() for n in ('wf-events.jsonl','events.jsonl','selector-disabled.log','corpus-events.jsonl','corpus-corrected-events.jsonl')}
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
