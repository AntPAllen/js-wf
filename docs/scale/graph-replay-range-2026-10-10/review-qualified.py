"""Independently review frozen range-export component qualification."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
state = json.loads((base / 'qualified-state.json').read_text())
root = Path(state['root'])
source = 'b3b9ce5894c6dacd1e197729a3c01eb5dee06422'
assert state['source'] == source and state['phase'] == 'terminal' and state['source_unchanged']
assert state['worker_complete'] and state['native_complete'] and state['corpus_complete']
assert len(state['commands']) == 6 and all(r['actual_exit_code'] == 0 and r['finished'] for r in state['commands'])
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
for name, provenance in state['binaries'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == provenance['sha256']
    assert ('-race=true' in provenance['build_info']) == provenance['race_instrumented'] == (name != 'sim-normal.test')
    compile_row = next(r for r in state['commands'] if r['command'][-1] == str(root / name) and '-c' in r['command'])
    assert ('-race' in compile_row['command']) == provenance['race_instrumented']
worker = (root / 'worker-race.log').read_text()
native = (root / 'native-cursor-race.log').read_text()
corpus = (root / 'corpus-normal.log').read_text()
for log in (worker, native, corpus):
    assert '--- FAIL:' not in log and 'WARNING: DATA RACE' not in log and log.rstrip().endswith('PASS')
assert 'range_gets=108 point_gets=237 elapsed=24.6s pin_ttl=4s pin_writes=11' in worker
assert len(re.findall(r'--- PASS: TestGraphReplaySnapshotRangeAndSlowAcquisition/\S+ ', worker)) == 2
assert len(re.findall(r'--- PASS: TestGraphReplaySnapshotOwnsInputsAndRejectedSignals/\S+ ', worker)) == 6
expected = {f'v{version}/R{replicas}-domain' for version in (4, 5, 6) for replicas in (1, 3)}
passed = re.findall(r'--- PASS: TestNativeGraphCursorOperatorHistory/(\S+) \(([0-9.]+)s\)', native)
assert len(passed) == 6 and {name for name, _ in passed} == expected
for version in (4, 5, 6):
    for replicas in (1, 3):
        assert len(re.findall(fr'CURSOR_REPLAY_EXPORT version={version} replicas={replicas} records=8 frame_bytes=\d+ input=7 readers=0', native)) == 1
    retained = 5 if version == 6 else 0
    assert native.count(f'NATIVE_CURSOR_OPERATOR version={version} records=8 retained_from={retained} original_entry_collected={str(version == 6).lower()} incorrect_schemas_rejected=2 domain_errors=0 legacy_journal_requests=0 readers_after=0') == 2
pins = {Path(n).name for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')}
observed = re.findall(r'--- PASS: TestPinnedRegressionCorpus/(\S+) ', corpus)
assert len(observed) == 841 and len(pins) == 841 and set(observed) == pins
assert next(r for r in state['commands'] if '-test.run=^TestPinnedRegressionCorpus$' in r['command'])['working_directory'] == str(Path(state['working_directory']) / 'sim')
negative = json.loads((base / 'old-negative.json').read_text())
assert negative['actual_exit_code'] == 1
assert (base / 'old-point-read.go.txt').read_bytes() == subprocess.check_output(['git', 'show', '502d510:worker/graph_replay.go'], cwd=repo)
assert '237 237' in (base / 'old-negative.log').read_text() and 'blob publication revoked' in (base / 'old-negative.log').read_text()
review = dict(verdict='PASS frozen ordered replay-export component', source=source, repository_inputs_git_verified_unchanged=len(required), native_cases={name: float(seconds) for name, seconds in passed}, records=33, range_gets=108, point_gets=237, acquisition_delay_seconds=3, simulated_total_seconds=24.6, pin_ttl_seconds=4, pin_writes=11, saved_regressions_normal=841, retained_binary_hashes={name: row['sha256'] for name, row in state['binaries'].items()}, raw_log_hashes={name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in ('worker-race.log', 'native-cursor-race.log', 'corpus-normal.log')}, old_negative_required_failures=True, scope='Directed exporter/uncertainty race controls, all six v4/v5/v6 R1/R3-domain native prepared-checkpoint history and owned frame/input replay exports, plus all841 normal pins at one frozen source. No SDK-produced whole-workflow offline replay/import/rollout or complete seeded/default/extended/whole-package race/native faults/scale/soak/public admission/collection/full goal acceptance.')
(base / 'qualified-review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
