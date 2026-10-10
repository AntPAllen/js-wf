"""Independent terminal review of the complete existing native archive matrix."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
state = json.loads((base / 'state.json').read_text())
root = Path(state['root'])
source = '57ee0b7187252a6a88b24ef655798cdfe3cf4b33'
assert state['source'] == source and state['phase'] == 'terminal'
assert state['command']['actual_exit_code'] == 0 and state['command']['finished']
assert state['complete_eight_cases'] and state['source_unchanged'] and state['binary_unchanged']
before = json.loads((root / 'source-before.json').read_text())
assert before == json.loads((root / 'source-after.json').read_text()) and before['source'] == source
names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', source], cwd=repo, text=True).splitlines()
required = {n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')}
assert set(before['files']) == required
batch = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    for name, expected in before['files'].items():
        batch.stdin.write((source + ':' + name + '\n').encode())
        batch.stdin.flush()
        header = batch.stdout.readline().split()
        assert len(header) == 3 and header[1] == b'blob', name
        content = batch.stdout.read(int(header[2]))
        assert batch.stdout.read(1) == b'\n'
        assert hashlib.sha256(content).hexdigest() == expected, name
finally:
    batch.stdin.close()
    assert batch.wait() == 0
prior = Path(state['binary_compile_receipts'])
assert before == json.loads((prior / 'source-before.json').read_text()) == json.loads((prior / 'source-after.json').read_text())
prior_base = base.parent / 'graph-continuation-cancel-pending-child-2026-10-10'
compile_state = json.loads((prior_base / 'state.json').read_text())
assert compile_state['source'] == source and compile_state['phase'] == 'terminal' and compile_state['source_unchanged']
assert compile_state['commands'][0]['actual_exit_code'] == 0
binary = Path(state['binary'])
assert compile_state['commands'][0]['command'] == ['go', 'test', '-c', '-race', './worker', '-o', str(binary)]
provenance = json.loads((root / 'binary.json').read_text())
assert provenance == json.loads((prior / 'binary.json').read_text()) == json.loads((prior_base / 'binary.json').read_text())
assert '-race=true' in provenance['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest() == provenance['sha256'] == '8e9763dcd9dd347f0226d4fd72e9d8b7ba55a1f5b64d524efafe0408b246e2a3'
assert state['command']['command'] == [str(binary), '-test.run=^TestNativeGraphContinuationArchiveCollection$', '-test.count=1', '-test.timeout=10m', '-test.v=true']
log = (root / 'race.log').read_text()
assert 'WARNING: DATA RACE' not in log and '--- FAIL:' not in log and log.rstrip().endswith('PASS')
expected = {f'{domain}/{mode}' for domain in ('R1', 'R3Domain') for mode in ('state', 'signals', 'child', 'buffered-child')}
passed = re.findall(r'--- PASS: TestNativeGraphContinuationArchiveCollection/(\S+) \(([0-9.]+)s\)', log)
assert len(passed) == 8 and {name for name, _ in passed} == expected
package = re.findall(r'^--- PASS: TestNativeGraphContinuationArchiveCollection \(([0-9.]+)s\)', log, re.M)
assert len(package) == 1
blocks = re.split(r'^=== RUN   TestNativeGraphContinuationArchiveCollection/', log, flags=re.M)[1:]
assert len(blocks) == 8
summaries = {}
for block in blocks:
    name = block.splitlines()[0]
    assert name in expected and name not in summaries
    mode = name.split('/')[1]
    counts = {'state': (8, 17, 20), 'signals': (9, 21, 26), 'child': (10, 20, 25), 'buffered-child': (10, 22, 25)}[mode]
    collected = re.findall(r'ARCHIVE_COLLECTION stage=(next|finish) original_entry_receipts_removed=(\d+) live_records=3 logical_records=(\d+)', block)
    assert len(collected) == 2 and [stage for stage, _, _ in collected] == ['next', 'finish']
    assert all(int(removed) > 0 for _, removed, _ in collected)
    assert tuple(int(logical) for _, _, logical in collected) == counts[:2]
    assert block.count(f'SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records={counts[2]} result=43') == 1
    child = mode in ('child', 'buffered-child')
    assert block.count('ARCHIVE_CHILD_RECLAIMED terminal_receipts=2 result_bytes=700000') == int(child)
    summaries[name] = dict(boundaries=[dict(stage=stage, obsolete_receipts_removed=int(removed), live_records=3, logical_records=int(logical)) for stage, removed, logical in collected], terminal_parent_records=counts[2], reclaimed_child_receipts=2 if child else 0)
review = dict(verdict='PASS complete eight-case native continuation archive matrix', source=source, repository_inputs_git_verified_unchanged=len(required), elapsed_seconds=float(package[0]), cases={name: float(seconds) for name, seconds in passed}, physical_and_logical_checks=summaries, binary_sha256=provenance['sha256'], log_sha256=hashlib.sha256(log.encode()).hexdigest(), actual_command_receipt=state['command'], scope='Existing eight-case healthy native archive matrix at unchanged60-second case/10-minute package watchdogs. Two collected/relocated checkpoint boundaries each; successful700KB cached/buffered child survives child terminal/result reclamation; immutable absolute logical history and one terminal outcome. No arbitrary faults, running cancellation, full retention/scale/soak, actual100000-entry boundary, canonical offline/import/rollout, public/default admission or full goal acceptance.')
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
