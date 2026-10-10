"""Review original diagnostic evidence after its retained supervisor exits."""
import collections
import calendar
import hashlib
import io
import json
from pathlib import Path
import subprocess
import math
import time

base = Path(__file__).resolve().parent
root = Path('/home/exedev/js-wf-partition-dispatch-gap-20261010')
state = json.loads((root / 'state.json').read_text())
checkout = Path(state['checkout'])
assert state['phase'] == 'closed' and state['sources_unchanged']
unit = 'js-wf-partition-dispatch-gap-v2-20261010.service'
raw = subprocess.check_output(['systemctl', '--user', 'show', unit, '-p', 'MainPID', '-p', 'LoadState', '-p', 'InvocationID', '-p', 'ExecMainStatus'], text=True)
properties = dict(line.split('=', 1) for line in raw.splitlines())
assert properties['LoadState'] == 'loaded' and properties['MainPID'] == '0'
assert properties['InvocationID'] == state['invocation']
assert int(properties['ExecMainStatus']) == state['native_exit']
assert len(state['commands']) == 4 and all(r['finished'] for r in state['commands'])
assert [r['exit'] for r in state['commands']] == [0, 0, 0, state['native_exit']]
before, after = [json.loads((root / n).read_text()) for n in ('source-before.json', 'source-after.json')]
assert before == after and before['source'] == state['source']
tracked = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', state['source']], cwd=checkout, text=True).splitlines()
names = [n for n in tracked if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
assert set(names) == set(before['files'])
stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=checkout, input=''.join(state['source'] + ':' + n + '\n' for n in names).encode()))
for name in names:
    header = stream.readline().decode().split()
    assert header[1] == 'blob'
    body = stream.read(int(header[2]))
    assert stream.read(1) == b'\n'
    expected = before['files'][name]
    assert len(body) == expected['bytes'] and hashlib.sha256(body).hexdigest() == expected['sha256']
    assert (checkout / name).read_bytes() == body
for name, expected in [('integration.test', state['sdk_binary']), ('candidate-server', state['candidate_restore']['binary'])]:
    body = (root / name).read_bytes()
    assert len(body) == expected['bytes'] and hashlib.sha256(body).hexdigest() == expected['sha256']
admission = json.loads((root / 'live-admission.json').read_text())
assert admission['invocation'] == state['invocation'] and admission['source'] == state['source']
assert len(admission['processes']) == 4
assert collections.Counter(Path(r['argv'][0]).name for r in admission['processes']) == {'integration.test': 1, 'candidate-server': 3}
for row in admission['processes']:
    expected = state['sdk_binary'] if row['argv'][0].endswith('integration.test') else state['candidate_restore']['binary']
    assert row['executable_sha256'] == expected['sha256']
    path = Path('/proc') / str(row['pid']) / 'stat'
    if path.exists():
        data = path.read_text()
        assert int(data[data.rindex(')') + 2:].split()[19]) != row['birth_ticks'], 'Original process remains alive'
rows = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
test = 'TestMixedMatrixServerPartitionEveryThirtySeconds'
assert not any(r['Action'] == 'skip' for r in rows)
outcome = 'pass' if state['native_exit'] == 0 else 'fail'
assert len([r for r in rows if r['Action'] == outcome and r.get('Test') == test]) == 1
partition = [json.loads(line) for line in (root / 'matrix-partition.jsonl').read_text().splitlines()]
open_pairs = {}
complete = []
for event in partition:
    key = (event['Worker'], event['Partition'], event['Attempt'], event['Operation'])
    assert event['Worker'] == 'matrix-worker' and event['Concurrency'] == 4
    assert 0 <= event['SlotsReserved'] <= 4 and event['Operation'] in ('pull', 'slot_wait')
    if event['Phase'] == 'begin':
        assert key not in open_pairs
        open_pairs[key] = event
    else:
        assert event['Phase'] == 'end' and key in open_pairs and event['Duration'] >= 0
        start = open_pairs.pop(key)
        complete.append(dict(begin=start, end=event))
assert not open_pairs and complete
faults = json.loads((root / 'matrix-faults.json').read_text())
assert faults['seed'] == 2 and faults['duration'] == '35s' and faults['faults']
assert all(f['majority_sequence'] > 0 and f['healed'] != '0001-01-01T00:00:00Z' for f in faults['faults'])
def ns(value):
    seconds, fraction = value.removesuffix('Z').split('.')
    return calendar.timegm(time.strptime(seconds, '%Y-%m-%dT%H:%M:%S')) * 10**9 + int(fraction.ljust(9, '0'))
latencies = json.loads((root / 'matrix-latencies.json').read_text())
for row in latencies:
    assert ns(row['observed']) - ns(row['enabled']) == row['delay_ns']
terminals = [r for r in latencies if r['event'] == 'terminal']
assert len(terminals) == 196 and len({(r['type'], r['id']) for r in terminals}) == 196
expected_counts = {'matrixshort': 28, 'matrixtimer': 21, 'matrixsignal': 14, 'matrixfanout': 7, 'matrixchild': 42, 'matrixgrandchild': 84}
assert dict(collections.Counter(r['type'] for r in terminals)) == expected_counts
cells = {}
for typ, count in expected_counts.items():
    delays = sorted(r['delay_ns'] for r in terminals if r['type'] == typ)
    cells[typ] = dict(invocations=count, raw_terminal_p99_ns=delays[math.ceil(count * .99) - 1])
    assert cells[typ]['raw_terminal_p99_ns'] < 30 * 10**9
evidence = ['state.json', 'live-admission.json', 'source-before.json', 'source-after.json', 'events.jsonl', 'matrix-partition.jsonl', 'matrix-faults.json', 'matrix-latencies.json', 'matrix-dispatch.jsonl', 'matrix-operations.jsonl', 'matrix-history.jsonl', 'sdk-build-info.log', 'candidate-build-info.log']
result = dict(source=state['source'], unit=unit, invocation=state['invocation'], supervisor=properties,
              git_verified_inputs=len(names), actual_original_processes_closed=True,
              native_outcome=outcome, native_exit=state['native_exit'], faults=faults,
              raw_latency_samples_verified=len(latencies), raw_terminal_cells=cells,
              evidence_sha256={n: hashlib.sha256((root / n).read_bytes()).hexdigest() for n in evidence},
              paired_operations=dict(collections.Counter(r['end']['Operation'] for r in complete)),
              longest_observations={op: sorted([r for r in complete if r['end']['Operation'] == op], key=lambda r: r['end']['Duration'], reverse=True)[:5] for op in ('pull', 'slot_wait')},
              scope=state['scope'], full200_accepted=False, original_seed2_failure_unchanged=True)
(base / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
(root / 'supervisor-exit.txt').write_text(raw)
print(json.dumps(result, indent=2))
