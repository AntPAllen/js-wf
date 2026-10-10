"""Independently review terminal native CLI replay evidence; never infer exit."""
import base64, datetime, hashlib, io, json, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
state = json.loads((base / 'state.json').read_text())
root, checkout = Path(state['root']), Path(state['checkout'])
assert state['exit'] == 0 and state['finished']
assert len(state['commands']) == 2 and all(c['exit'] == 0 for c in state['commands'])
assert state['commands'][0]['command'] == ['go', 'test', '-race', '-c', '-o', str(root / 'worker-race.test'), './worker']
assert state['commands'][1]['cwd'] == str(checkout / 'worker')
assert '-test.run=^TestNativeGraphContinuation(ChildOfflineReplay|OfflineReplay)$' in state['commands'][1]['command']
assert '-test.timeout=30m' in state['commands'][1]['command']
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
for name, artifact in state['artifacts'].items():
    path = root / name
    assert path.stat().st_size == artifact['bytes']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact['sha256']
    if path.name in ('worker-race.test', 'wf', 'handler.so'):
        info = subprocess.check_output(['go', 'version', '-m', str(path)], text=True)
        assert '-race=true' in info
rows = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
assert not any(r['Action'] in ('fail', 'skip') for r in rows)
prefix = 'TestNativeGraphContinuationOfflineReplay'
child_prefix = 'TestNativeGraphContinuationChildOfflineReplay'
state_cases = [prefix + '/R' + str(r) + '-domain/archive=' + str(a).lower() for r in (1, 3) for a in (False, True)]
child_cases = [child_prefix + '/R' + str(r) + '-domain/archive=' + str(a).lower() + '/cached=' + str(c).lower() + '/failed=' + str(f).lower() for r in (1, 3) for a in (False, True) for c in (False, True) for f in (False, True)]
cases = state_cases + child_cases
passed = [r['Test'] for r in rows if r['Action'] == 'pass' and r.get('Test') in cases]
assert sorted(passed) == sorted(cases)
for top in (prefix, child_prefix):
    assert any(r['Action'] == 'pass' and r.get('Test') == top for r in rows)
assert any(r['Action'] == 'pass' and 'Test' not in r for r in rows)
case_receipts = {}
for case in cases:
    output = ''.join(r.get('Output', '') for r in rows if r.get('Test') == case)
    child = case in child_cases
    cached = '/cached=true/' in case
    failed = case.endswith('failed=true')
    assert output.count('OFFLINE_REPLAY status=suspended') == (2 if child and not cached else 1)
    assert output.count('OFFLINE_REPLAY status=completed') == 1
    assert 'result=60' in output and 'waiting_on=signal:gate' in output
    assert output.count('OFFLINE_ARCHIVE') == (2 if '/archive=true' in case else 0)
    if child:
        assert output.count('OFFLINE_CHILD_CHECKPOINT') == 2
        assert 'count=1 cached_outcomes=0 child_calls=0' in output
        assert ('count=2 cached_outcomes=1 child_calls=1' if cached else 'count=2 cached_outcomes=0 child_calls=0') in output
        assert output.count('OFFLINE_CHILD_RECLAIMED') == 1 and 'compatibility_source_removed=true' in output
        assert ('result_bytes=0' if failed else 'result_bytes=700000') in output
        assert output.count('OFFLINE_CHILD_COMPLETE') == 1
        assert ('finish=2' if cached else 'finish=3') in output
        assert ('negative_controls=2' if failed else 'negative_controls=3') in output
    else:
        assert output.count('OFFLINE_COMPLETE') == 1 and 'negative_controls=2' in output
    case_receipts[case] = output
# All artifacts are retained and their byte hashes bound before inspection.
names = ('suspended.json', 'completed.json', 'child-pending.json', 'missing-objects.json', 'missing-child-result.json')
exports = [p for p in (root / 'artifacts').rglob('*.json') if p.name in names]
assert len(exports) == 76
assert len([name for name in state['artifacts'] if Path(name).name in ('worker-race.test', 'wf', 'handler.so')]) == 5
assert not list((root / 'artifacts').rglob('effect'))
counts = {}
for path in exports:
    bundle = json.loads(path.read_text())
    assert bundle['type'] == 'continued' and bundle['id'] == 'offline' and bundle['inv_seq'] == 1
    raw_input = base64.b64decode(bundle['input'])
    assert hashlib.sha256(raw_input).hexdigest() == bundle['input_hash']
    inp = json.loads(raw_input)
    child = isinstance(inp, dict)
    if child:
        assert set(inp) == {'value', 'cached', 'failed'} and inp['value'] == 7
        cached, failed = inp['cached'], inp['failed']
        assert type(cached) is bool and type(failed) is bool
    else:
        assert inp == 7
        cached = failed = None
    counts[(path.name, cached, failed)] = counts.get((path.name, cached, failed), 0) + 1
    records = bundle['journal']
    for i, record in enumerate(records):
        assert record['index'] == i and record['sequence'] == i + 1
        assert i == 0 or record['epoch'] >= records[i-1]['epoch']
    assert records[0]['kind'] == 'Started'
    if path.name in ('suspended.json', 'child-pending.json'):
        assert records[-1]['kind'] == 'Suspended'
        wait = records[-1]['payload']['waiting_on']
        assert wait == 'signal:gate' if path.name == 'suspended.json' else wait.startswith('signal:child_')
    else:
        assert records[-1]['kind'] == 'Completed' and base64.b64decode(records[-1]['payload']['result']) == b'60'
    if not child:
        assert len(records) == (13 if path.name == 'suspended.json' else 18)
    else:
        expected_records = (20 if cached else 21) if path.name == 'suspended.json' else 17 if path.name == 'child-pending.json' else 25 if cached else 26
        assert len(records) == expected_records
    if path.name == 'missing-objects.json':
        assert bundle['objects'] == {}
        continue
    frames = []
    child_ref = None
    for record in records:
        if record['kind'] == 'SignalConsumed' and record['payload'].get('graph_child'):
            declaration = record['payload']['graph_child']
            assert declaration['type'] == 'offlinechild' and declaration['inv_seq'] == 2
            event = record['payload']
            body = base64.b64decode(bundle['objects'][event['ref']]) if event.get('ref') else base64.b64decode(event['payload'])
            assert hashlib.sha256(body).hexdigest() == event['hash']
            outcome = json.loads(body)
            assert outcome['inv_seq'] == declaration['inv_seq']
            if failed:
                assert outcome['error'] == 'planned offline child failure'
                assert not outcome.get('result_ref') and not declaration.get('result_ref')
            else:
                assert not outcome.get('error')
                assert outcome['result_ref'] == declaration['result_ref'] and outcome['result_hash'] == declaration['result_hash']
            if not failed:
                child_ref = declaration['result_ref']
                if path.name == 'missing-child-result.json':
                    assert child_ref not in bundle['objects']
                else:
                    owned = base64.b64decode(bundle['objects'][child_ref])
                    assert owned == json.dumps('x' * 700000).encode()
                    assert hashlib.sha256(owned).hexdigest() == declaration['result_hash']
    if child and path.name != 'child-pending.json':
        assert sum(r['kind'] == 'SignalConsumed' and bool(r['payload'].get('graph_child')) for r in records) == 1
        if not failed:
            assert child_ref
    for name, value in bundle['objects'].items():
        body = base64.b64decode(value)
        try:
            data = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(data, dict) and 'stage' in data:
            assert name == 'step-result-' + hashlib.sha256(body).hexdigest()
            assert data['identity'] == dict(type='continued', id='offline', inv_seq=1)
            assert data['state'] == dict(value=23)
            if child:
                local = data['data']
                assert local['promise']['ChildType'] == 'offlinechild'
                assert local['promise']['ChildID'] and local['promise']['SignalName']
                expected = 1 if cached and data['stage'] == 'child_finish_v1' else 0
                assert len(data.get('promise_outcomes') or {}) == expected
                frames.append((data['stage'], local['count']))
            else:
                frames.append((data['stage'], data['data']))
    assert sorted(frames) == ([('child_finish_v1', 2), ('child_middle_v1', 1)] if child else [('finish_v1', 30), ('middle_v1', 7)])
assert all(n == 4 for n in counts.values())
assert len(counts) == 19
result = dict(accepted=True, source=state['source'], git_verified_inputs=len(names), cases=cases, package_elapsed=[r['Elapsed'] for r in rows if r['Action'] == 'pass' and 'Test' not in r][0], events_sha256=hashlib.sha256((root / 'events.jsonl').read_bytes()).hexdigest(), reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(), scope='Sixteen healthy pending/cached success/failure child offline replay cases, plus four state-only regressions. Public admission, import, rollout, fault/buffered-signal matrices and broader original requirements remain open.')
(base / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
