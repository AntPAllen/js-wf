"""Verify the actual executable, active diagnostic, and live full launch."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

out = Path(__file__).resolve().parent
prepared = json.loads((out / 'preparation.json').read_text())
root, frozen = Path(prepared['root']), Path(prepared['snapshot'])
inputs = json.loads((out / 'source-inputs.json').read_text())
names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', prepared['revision']], text=True).splitlines()
assert set(inputs) == {n for n in names if n.endswith(('.go', '.yml')) or n in ('go.mod', 'go.sum')}
for name, sha in inputs.items():
    original = subprocess.check_output(['git', 'show', prepared['revision'] + ':' + name])
    assert hashlib.sha256(original).hexdigest() == sha
    assert hashlib.sha256((frozen / name).read_bytes()).hexdigest() == sha
build = json.loads((out / 'build-command.json').read_text())
assert build['actual_exit'] == 0 and build['source_unchanged']
assert hashlib.sha256((root / 'integration.test').read_bytes()).hexdigest() == build['binary_sha256']
preflight = json.loads((out / 'preflight-command.json').read_text())
assert preflight['actual_exit'] == 0 and not Path('/proc/' + str(preflight['pid'])).exists()
assert preflight['binary_sha256'] == build['binary_sha256']
log = (out / 'preflight.log').read_text()
assert 'count=10 reuse=1 active=true full_scale=false' in log
assert 'kept 10 other handlers executing before purge' in log
assert 'purged 10 while 10 remained live; reused 1 ids' in log
assert '--- PASS: TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs ' in log
report = json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})', log)[1])
assert all(report[k] == 11 for k in ('Invocations', 'Journals', 'Terminal'))
assert report['Entries'] == 66 and report['Retired'] == report['RetiredTombstones'] == 9
assert report['References']['ReaderPins'] == 0
native = Path(preflight['native_root'])
actual = {str(p.relative_to(native)): dict(bytes=p.stat().st_size,
          sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in native.rglob('*') if p.is_file()}
assert actual == json.loads((out / 'preflight-census.json').read_text())
launch = json.loads((out / 'launch.json').read_text())
assert hashlib.sha256((out / 'supervisor.py').read_bytes()).hexdigest() == launch['supervisor_sha256']
state = json.loads((root / 'state.json').read_text())
assert state['invocation'] == launch['invocation'] and state['revision'] == prepared['revision']
assert state['source_inputs'] == inputs and state['binary_sha256'] == build['binary_sha256']
assert not state['finished'] and state['actual_exit'] is None, 'campaign is no longer live'
for key in ('supervisor_pid', 'driver_pid'):
    assert state[key] == launch[key] and Path('/proc/' + str(state[key])).exists()
driver = Path('/proc/' + str(state['driver_pid']))
assert driver.joinpath('stat').read_text().split()[21] == state['driver_proc_start'] == launch['driver_proc_start']
assert driver.joinpath('cmdline').read_bytes().split(b'\0')[:-1] == [a.encode() for a in state['command']]
assert hashlib.sha256(driver.joinpath('exe').read_bytes()).hexdigest() == build['binary_sha256']
result = dict(prepared_verified=True, active10_diagnostic_accepted=True, full_accepted=False,
              revision=prepared['revision'], inputs=len(inputs), binary_sha256=build['binary_sha256'],
              preflight_native_files=len(actual), preflight_native_bytes=preflight['native_bytes'],
              supervisor_pid=state['supervisor_pid'], driver_pid=state['driver_pid'], invocation=state['invocation'])
(out / 'prepared-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
