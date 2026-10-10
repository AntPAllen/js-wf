"""Independently review terminal native CLI replay evidence; never infer exit."""
import datetime, hashlib, io, json, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
state = json.loads((base / 'state.json').read_text())
root, checkout = Path(state['root']), Path(state['checkout'])
assert state['exit'] == 0 and state['finished']
assert len(state['commands']) == 5 and all(c['exit'] == 0 for c in state['commands'])
assert state['commands'][1]['command'] == ['go', 'test', '-race', '-c', '-o', str(root / 'cli-race.test'), './cmd/wf']
assert state['commands'][2]['cwd'] == str(checkout / 'cmd/wf')
assert '-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin)$' in state['commands'][2]['command']
assert '-test.timeout=10m' in state['commands'][2]['command']
assert state['commands'][3]['command'] == ['go','build','-race','-o',str(root/'wf'),'./cmd/wf']
assert state['commands'][4]['command'] == ['go','build','-race','-buildmode=plugin','-o',str(root/'handler.so'),'./worker/testdata/graphreplayplugin']
before, after = [json.loads((root / name).read_text()) for name in ['source-before.json', 'source-after.json']]
assert before == after and before['source'] == state['source']
names = list(before['files'])
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
    assert '-race=true' in info
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
original=subprocess.check_output(['git','show','0e37cc1:docs/scale/graph-continuation-child-offline-2026-10-10/review.py'],cwd=checkout)
assert hashlib.sha256(original).hexdigest()==state['native_review_sha256']
old=json.loads((base/'diagnostic/report.json').read_text())
for key in ('cli','plugin'):
    assert hashlib.sha256(Path(old[key]).read_bytes()).hexdigest()==old[key+'_sha256']
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(before['files']),native_source=state['native_source'],native_git_verified_inputs=3312,directed_controls=20,legacy_continuation_plugin_pass=True,offline_cli_runs=106,positive_native_replays=48,negative_native_objects=28,negative_native_stages=20,malformed_new_rejections=4,malformed_old_acceptances=4,events_sha256=hashlib.sha256((root/'events.jsonl').read_bytes()).hexdigest(),reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Graph child request/outcome/result provenance binding and all retained healthy native bundles only. Full offline/fault/import/rollout and broader original requirements remain open.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
