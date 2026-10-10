"""Independently review terminal native CLI replay evidence; never infer exit."""
import datetime, hashlib, io, json, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
state = json.loads((base / 'corrected-state.json').read_text())
root, checkout = Path(state['root']), Path(state['checkout'])
assert state['exit'] == 0 and state['finished']
assert len(state['commands']) == 2 and all(c['exit'] == 0 for c in state['commands'])
assert state['commands'][0]['command'] == ['go', 'test', '-race', '-c', '-o', str(root / 'worker-race.test'), './worker']
assert state['commands'][1]['cwd'] == str(checkout / 'worker')
assert '-test.run=^TestNativeGraphContinuationOfflineReplay$' in state['commands'][1]['command']
assert '-test.timeout=10m' in state['commands'][1]['command']
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
cases = [prefix + '/R' + str(r) + '-domain/archive=' + str(a).lower() for r in (1, 3) for a in (False, True)]
assert sorted(r['Test'] for r in rows if r['Action'] == 'pass' and '/' in r.get('Test', '')) == sorted(cases)
assert any(r['Action'] == 'pass' and r.get('Test') == prefix for r in rows)
assert any(r['Action'] == 'pass' and 'Test' not in r for r in rows)
for case in cases:
    output = ''.join(r.get('Output', '') for r in rows if r.get('Test') == case)
    assert output.count('OFFLINE_REPLAY status=suspended') == 1
    assert output.count('OFFLINE_REPLAY status=completed') == 1
    assert 'result=60' in output and 'waiting_on=signal:gate' in output
    assert output.count('OFFLINE_COMPLETE') == 1 and 'negative_controls=2' in output
    assert output.count('OFFLINE_ARCHIVE') == (2 if case.endswith('true') else 0)
# All four cases retain two real exports and one deliberate missing-object edit.
exports = [p for p in (root / 'artifacts').rglob('*.json') if p.name in ('suspended.json', 'completed.json', 'missing-objects.json')]
assert len(exports) == 12
result = dict(accepted=True, source=state['source'], git_verified_inputs=len(names), cases=cases, package_elapsed=[r['Elapsed'] for r in rows if r['Action'] == 'pass' and 'Test' not in r][0], events_sha256=hashlib.sha256((root / 'events.jsonl').read_bytes()).hexdigest(), reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(), scope='Four healthy SDK-generated canonical continuation offline replay cases only; public admission, import, rollout, faults and broader original requirements remain open.')
(base / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
