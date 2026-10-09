import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

base = Path(__file__).resolve().parent
repo = base.parents[2]
checkout = Path('/home/exedev/js-wf-current151-race-qualification')
root = Path('/home/exedev/js-wf-tier1-full151-race1000-after-reboot-20261009')
normal = Path('/home/exedev/js-wf-tier1-full151-normal1000-20261009')
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip()
assert revision == 'fb4f08603ffd9d0ec8bc08eaa39e13a2066e8b34'
assert not root.exists() and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
def identity(pid):
    proc = Path('/proc') / str(pid)
    try:
        fields = (proc / 'stat').read_text().split(') ', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return {'pid': pid, 'start_ticks': fields[19], 'arguments': [arg.decode() for arg in (proc / 'cmdline').read_bytes().split(b'\0') if arg]}
    except FileNotFoundError:
        return None
observed = identity(3257590)
if observed is not None:
    assert observed['arguments'] == ['python3', 'docs/scale/graph-current151-qualification-2026-10-09/run-normal.py'], observed
command = ['python3', 'scripts/check-tier1-race.py', '--root', str(root), '--seeds', '1000']
record = {'source': revision, 'command': command, 'working_directory': str(checkout), 'root': str(root), 'queued_at': stamp(), 'wait_for_observed_normal_supervisor': observed, 'test_watchdog_minutes': 300, 'scope': 'Complete current 151-family/all-810-pin race, after full normal runner success; production Go/trace cohort equals frozen normal source.'}
(base / 'race-after-reboot-launch.json').write_text(json.dumps(record, indent=2) + '\n')
while observed is not None and identity(observed['pid']) == observed:
    time.sleep(5)
try:
    launch = json.loads((base / 'normal-launch.json').read_text())
    assert launch['exit_code'] == 0, 'normal runner did not succeed'
    result = json.loads((normal / 'tier1-result.json').read_text())
    assert result['source'] == 'e5018af37cc85185b331c1aa4936d0c2cf4c74c3'
    assert result['pinned_regressions_pass'] == 810
    proof = result['per_workload_seed_proof']
    assert proof['workloads'] == 151 and proof['completed_bodies'] == 151000
    assert proof['first'] == 1 and proof['last'] == 1000
    before = json.loads((normal / 'source-before.json').read_text())
    assert before == json.loads((normal / 'source-after.json').read_text())
    selected = {name: digest for name, digest in before['files'].items() if name.endswith('.go') or name in ('go.mod', 'go.sum') or name.startswith('sim/testdata/')}
    for name, digest in selected.items():
        assert hashlib.sha256((checkout / name).read_bytes()).hexdigest() == digest, name
    record['normal_production_go_and_trace_inputs_matched'] = len(selected)
    record['normal_result_sha256'] = hashlib.sha256((normal / 'tier1-result.json').read_bytes()).hexdigest()
except Exception as error:
    record['not_started_at'] = stamp()
    record['not_started_reason'] = str(error)
    (base / 'race-after-reboot-launch.json').write_text(json.dumps(record, indent=2) + '\n')
    raise
record['started_at'] = stamp()
(base / 'race-after-reboot-launch.json').write_text(json.dumps(record, indent=2) + '\n')
with (base / 'race-after-reboot-supervisor.log').open('w') as out:
    child = subprocess.run(command, cwd=checkout, stdout=out, stderr=subprocess.STDOUT)
record['finished_at'] = stamp()
record['exit_code'] = child.returncode
(base / 'race-after-reboot-launch.json').write_text(json.dumps(record, indent=2) + '\n')
print('race supervisor actual exit', child.returncode)
