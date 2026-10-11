import hashlib
import json
from pathlib import Path
import subprocess
import time

out = Path(__file__).resolve().parent
prepared = json.loads((out / 'preparation.json').read_text())
root = Path(prepared['root'])
unit = 'js-wf-canonical-purge10000-phases-20261011.service'
assert 'LoadState=not-found\n' in subprocess.check_output(
    ['systemctl', '--user', 'show', unit, '-p', 'LoadState'], text=True)
preflight = json.loads((out / 'preflight-command.json').read_text())
assert preflight['actual_exit'] == 0
log = (out / 'preflight.log').read_text()
assert 'count=10 reuse=1 active=true full_scale=false' in log
assert 'kept 10 other handlers executing before purge' in log
assert '--- PASS: TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs ' in log
command = ['systemd-run', '--user', '--unit', unit, '--property=CPUQuota=200%',
           '--property=MemoryMax=4G', '--property=RuntimeMaxSec=1800', '--property=RemainAfterExit=yes',
           '--property=WorkingDirectory=/home/exedev/js-wf', '--property=Environment=PYTHONUNBUFFERED=1',
           '/usr/bin/python3', str(out / 'supervisor.py')]
start = time.time_ns()
result = subprocess.run(command, capture_output=True, text=True)
assert result.returncode == 0, result.stderr
for _ in range(100):
    path = root / 'state.json'
    if path.exists():
        state = json.loads(path.read_text())
        if state.get('driver_pid'):
            break
    time.sleep(.1)
else:
    raise RuntimeError('supervisor did not bind actual driver')
assert not state['finished'] and state['actual_exit'] is None
for key in ('supervisor_pid', 'driver_pid'):
    assert Path('/proc/' + str(state[key])).exists()
props = subprocess.check_output(['systemctl', '--user', 'show', unit,
    '-p', 'MainPID', '-p', 'ExecMainPID', '-p', 'ExecStart', '-p', 'ExecMainStatus',
    '-p', 'InvocationID', '-p', 'CPUQuotaPerSecUSec', '-p', 'MemoryMax',
    '-p', 'RuntimeMaxUSec', '-p', 'RemainAfterExit', '-p', 'ActiveState', '-p', 'SubState'], text=True)
service = dict(line.split('=', 1) for line in props.splitlines())
assert service['ActiveState'] == 'active' and service['SubState'] == 'running'
assert int(service['MainPID']) == state['supervisor_pid']
assert service['InvocationID'] == state['invocation']
assert service['CPUQuotaPerSecUSec'] == '2s' and service['MemoryMax'] == '4294967296'
assert service['RuntimeMaxUSec'] == '30min'
actual_argv = Path('/proc/' + str(state['driver_pid']) + '/cmdline').read_bytes().split(b'\0')[:-1]
assert [x.decode() for x in actual_argv] == state['command']
assert hashlib.sha256(Path('/proc/' + str(state['driver_pid']) + '/exe').read_bytes()).hexdigest() == state['binary_sha256']
receipt = dict(unit=unit, invocation=state['invocation'], revision=state['revision'],
               supervisor_pid=state['supervisor_pid'], driver_pid=state['driver_pid'],
               driver_proc_start=state['driver_proc_start'], service=service,
               launch_command=command, actual_launch_exit=0, start_ns=start, end_ns=time.time_ns(),
               supervisor_sha256=hashlib.sha256((out / 'supervisor.py').read_bytes()).hexdigest(),
               accepted_full=False)
(out / 'launch.json').write_text(json.dumps(receipt, indent=2) + '\n')
(out / 'loaded-unit.txt').write_text(props)
print(json.dumps(receipt))
