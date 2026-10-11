"""Retain and verify terminal evidence; delegate success to the full-count gate."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import time

out = Path(__file__).resolve().parent
prepared = json.loads((out / 'preparation.json').read_text())
root = Path(prepared['root'])
state = json.loads((root / 'state.json').read_text())
assert state['finished'] and isinstance(state['actual_exit'], int), 'campaign is not closed'
for key in ('supervisor_pid', 'driver_pid'):
    assert not Path('/proc/' + str(state[key])).exists(), key
launch = json.loads((out / 'launch.json').read_text())
assert state['invocation'] == launch['invocation']
assert hashlib.sha256((out / 'supervisor.py').read_bytes()).hexdigest() == launch['supervisor_sha256']
assert state['binary_sha256'] == json.loads((out / 'build-command.json').read_text())['binary_sha256']
assert hashlib.sha256(Path(state['binary']).read_bytes()).hexdigest() == state['binary_sha256']
inputs = json.loads((out / 'source-inputs.json').read_text())
assert state['source_inputs'] == inputs
for name, sha in inputs.items():
    assert hashlib.sha256((Path(state['snapshot']) / name).read_bytes()).hexdigest() == sha
native = Path(state['native_root'])
census = json.loads((root / 'native-census.json').read_text())
actual = {str(p.relative_to(native)): dict(bytes=p.stat().st_size,
          sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in native.rglob('*') if p.is_file()}
assert census == actual and len(actual) == state['native_files']
assert sum(v['bytes'] for v in actual.values()) == state['native_bytes']
props = subprocess.check_output(['systemctl', '--user', 'show', launch['unit'],
    '-p', 'MainPID', '-p', 'ExecMainStatus', '-p', 'InvocationID', '-p', 'ActiveState',
    '-p', 'SubState', '-p', 'ExecMainExitTimestamp', '-p', 'ExecMainPID'], text=True)
assert 'MainPID=0\n' in props and 'ExecMainStatus=' + str(state['actual_exit'] % 256) + '\n' in props
assert 'InvocationID=' + state['invocation'] + '\n' in props
log = (root / 'full.log').read_text()
assert 'count=10000 reuse=1000 active=true full_scale=true' in log
if state['actual_exit']:
    assert log.strip()
start = time.time_ns()
review = subprocess.run(['python3', str(out / 'full-review.py'), str(root)], capture_output=True, text=True)
assert review.returncode == (0 if state['actual_exit'] == 0 else 1)
(out / 'terminal-state.json').write_bytes((root / 'state.json').read_bytes())
(out / 'terminal-full.log').write_text(log)
(out / 'terminal-unit.txt').write_text(props)
(out / 'terminal-census.json.gz').write_bytes(gzip.compress((root / 'native-census.json').read_bytes(), mtime=0))
(out / 'terminal-full-review.log').write_text(review.stdout + review.stderr)
result = dict(terminal_evidence_verified=True, actual_exit=state['actual_exit'],
              full_accepted=state['actual_exit'] == 0, revision=state['revision'],
              invocation=state['invocation'], inputs=len(inputs),
              binary_sha256=state['binary_sha256'], native_files=len(actual),
              native_bytes=state['native_bytes'], wall_seconds=(state['end_ns'] - state['start_ns']) / 1e9,
              full_review_actual_exit=review.returncode, review_start_ns=start, review_end_ns=time.time_ns(),
              cause_confirmed=False)
(out / 'terminal-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
