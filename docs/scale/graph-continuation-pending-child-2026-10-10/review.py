"""Accept only terminal, source-bound pending-child native matrix evidence."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
state = json.loads((base / 'state.json').read_text())
root = Path(state['root'])
source = '0bf850c37089f25be6b7d2167f2c605cd7dbfbb4'
assert state['source'] == source and state['phase'] == 'terminal'
assert state['test_exit'] == 0 and state['complete_eight_cases'] and state['source_unchanged']
assert len(state['commands']) == 2 and all(r['actual_exit_code'] == 0 and r['finished'] for r in state['commands'])
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
changed = subprocess.check_output(['git', 'diff', '--name-only', 'd504a33', source, '--', '*.go', 'go.mod', 'go.sum', 'sim/testdata/'], cwd=repo, text=True).splitlines()
assert changed == ['worker/graph_continuation_sdk_test.go']
binary = json.loads((root / 'binary.json').read_text())
assert '-race=true' in binary['build_info']
assert hashlib.sha256((root / 'worker-race.test').read_bytes()).hexdigest() == binary['sha256']
assert state['commands'][0]['command'] == ['go', 'test', '-c', '-race', './worker', '-o', str(root / 'worker-race.test')]
assert state['commands'][1]['command'] == [str(root / 'worker-race.test'), '-test.run=^TestNativeGraphContinuationPendingChildAcrossCheckpoints$', '-test.count=1', '-test.timeout=10m', '-test.v=true']
log = (root / 'race.log').read_text()
assert 'WARNING: DATA RACE' not in log and '--- FAIL:' not in log and log.rstrip().endswith('PASS')
expected = {f'{domain}/archive={archive}/failed={failed}' for domain in ('R1', 'R3Domain') for archive in ('false', 'true') for failed in ('false', 'true')}
passed = re.findall(r'--- PASS: TestNativeGraphContinuationPendingChildAcrossCheckpoints/(\S+) \(([0-9.]+)s\)', log)
assert len(passed) == 8 and {name for name, _ in passed} == expected
package = re.findall(r'^--- PASS: TestNativeGraphContinuationPendingChildAcrossCheckpoints \(([0-9.]+)s\)', log, re.M)
assert len(package) == 1
assert log.count('PENDING_CHILD_CHECKPOINT stage=next locals_count=1 child_calls=0 outcomes=0 buffered_signals=0') == 8
assert log.count('PENDING_CHILD_CHECKPOINT stage=finish locals_count=2 child_calls=0 outcomes=0 buffered_signals=0') == 8
assert len(re.findall(r'PENDING_CHILD_SUSPENDED records=\d+ effects=2 child_calls=0 waiting_on=signal:child_\d+', log)) == 8
assert log.count('PENDING_CHILD_WAKEUP_RECOVERED reenqueued=1') == 8
resumed = re.findall(r'PENDING_CHILD_RESUMED failed=(true|false) archive=(true|false) prefix_records=(\d+) records=(\d+) initial=1 next=1 finish=2 effects=2 child_calls=1 result=43', log)
assert len(resumed) == 8
for failed in ('false', 'true'):
    for archive in ('false', 'true'):
        assert len([r for r in resumed if r[:2] == (failed, archive)]) == 2
assert all(int(prefix) == 21 and int(records) == 26 for _, _, prefix, records in resumed)
assert log.count('ARCHIVE_COLLECTION stage=next ') == 4 and log.count('ARCHIVE_COLLECTION stage=finish ') == 4
assert log.count('SDK initial/next/finish=map[finish:2 initial:1 next:1] effects=2 ') == 8
assert log.count('child_calls=1 preserved_error=planned_child_failure parent_result=43') == 4
review = dict(verdict='PASS eight native pending-child continuation cases', source=source, repository_inputs_git_verified_unchanged=len(required), production_and_simulation_inputs_unchanged_from='d504a33', elapsed_seconds=float(package[0]), cases={name: float(seconds) for name, seconds in passed}, binary_sha256=binary['sha256'], log_sha256=hashlib.sha256(log.encode()).hexdigest(), command_receipts=state['commands'], scope='Two unresolved SDK promise checkpoints; live/archive boundaries; actual parent suspension; success700KB/failure child; canonical signal wakeup repair; immutable logical prefix; one child/two effects/one terminal outcome and fenced duplicate. No child terminal reclamation, process/storage/VM fault, broad scale/soak, actual100000-entry cap, public/default admission or full goal acceptance.')
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
