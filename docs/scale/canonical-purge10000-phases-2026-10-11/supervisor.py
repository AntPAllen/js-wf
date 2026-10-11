"""Run the frozen full active-handler fixture; retain actual closure on failure."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

out = Path(__file__).resolve().parent
prepared = json.loads((out / 'preparation.json').read_text())
root, frozen = Path(prepared['root']), Path(prepared['snapshot'])
assert not (root / 'state.json').exists() and not (root / 'native').exists()
assert shutil.disk_usage(root).free >= 32 * 1024**3
inputs = json.loads((out / 'source-inputs.json').read_text())
for name, sha in inputs.items():
    assert hashlib.sha256((frozen / name).read_bytes()).hexdigest() == sha, name
build = json.loads((out / 'build-command.json').read_text())
assert build['actual_exit'] == 0 and build['source_unchanged']
preflight = json.loads((out / 'preflight-command.json').read_text())
assert preflight['actual_exit'] == 0 and preflight['binary_sha256'] == build['binary_sha256']
binary = root / 'integration.test'
assert hashlib.sha256(binary.read_bytes()).hexdigest() == build['binary_sha256']
native = root / 'native'
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='2GiB',
           WF_GRAPH_PURGE_REUSE_SCALE='1', WF_GRAPH_PURGE_REUSE_NATIVE_ROOT=str(native),
           WF_PURGE_REUSE_ACTIVE='1')
for key in ('WF_PURGE_REUSE_COUNT', 'WF_PURGE_REUSE_POSTGRES', 'WF_PURGE_REUSE_SCALE'):
    env.pop(key, None)
command = [str(binary), '-test.run=^TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs$',
           '-test.count=1', '-test.v=true', '-test.timeout=22m']
state = dict(revision=prepared['revision'], source_inputs=inputs,
             snapshot=str(frozen), binary=str(binary), binary_sha256=build['binary_sha256'],
             native_root=str(native), supervisor_pid=os.getpid(),
             invocation=os.environ.get('INVOCATION_ID'), command=command,
             environment={k: env[k] for k in ('GOMAXPROCS', 'GOMEMLIMIT', 'WF_GRAPH_PURGE_REUSE_SCALE',
                                             'WF_GRAPH_PURGE_REUSE_NATIVE_ROOT', 'WF_PURGE_REUSE_ACTIVE')},
             start_ns=time.time_ns(), finished=False, actual_exit=None)

def save():
    temporary = root / 'state.json.tmp'
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(root / 'state.json')

with (root / 'full.log').open('wb') as log:
    child = subprocess.Popen(command, cwd=frozen, env=env, stdout=log, stderr=subprocess.STDOUT)
    state.update(driver_pid=child.pid,
                 driver_proc_start=Path('/proc/' + str(child.pid) + '/stat').read_text().split()[21])
    save()
    state['actual_exit'] = child.wait()
state['end_ns'] = time.time_ns()
save()
for name, sha in inputs.items():
    assert hashlib.sha256((frozen / name).read_bytes()).hexdigest() == sha, name
census = {str(p.relative_to(native)): dict(bytes=p.stat().st_size,
          sha256=hashlib.sha256(p.read_bytes()).hexdigest())
          for p in sorted(native.rglob('*')) if p.is_file()}
(root / 'native-census.json').write_text(json.dumps(census, indent=2) + '\n')
state.update(native_files=len(census), native_bytes=sum(v['bytes'] for v in census.values()), finished=True)
save()
sys.exit(state['actual_exit'])
