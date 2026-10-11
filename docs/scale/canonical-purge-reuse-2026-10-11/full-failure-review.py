import gzip,hashlib,json,re
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-canonical-purge10000-active-20261011')
state=json.loads((out/'full-terminal-state.json').read_text())
assert (root/'state.json').read_bytes()==(out/'full-terminal-state.json').read_bytes()
assert state['finished'] and state['actual_exit']==1
assert not Path('/proc/'+str(state['driver_pid'])).exists() and not Path('/proc/'+str(state['supervisor_pid'])).exists()
assert hashlib.sha256(Path(state['binary']).read_bytes()).hexdigest()==state['binary_sha256']
for name,sha in state['source_inputs'].items():assert hashlib.sha256((Path(state['snapshot'])/name).read_bytes()).hexdigest()==sha,name
census=json.loads(gzip.decompress((out/'full-terminal-native-census.json.gz').read_bytes()))
native=Path(state['native_root']);actual={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in native.rglob('*') if p.is_file()}
assert actual==census and len(actual)==2443 and sum(v['bytes'] for v in actual.values())==198307813
log=(out/'full-terminal-full.log').read_text();assert log==(root/'full.log').read_text()
assert 'count=10000 reuse=1000 active=true full_scale=true' in log
assert 'start outcome unknown: input: context deadline exceeded' in log
assert '--- FAIL: TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs ' in log
assert 'completed 10000 old invocations' not in log and 'canonical raw purge/reuse audit=' not in log
unit=(out/'full-terminal-unit.txt').read_text()
assert 'MainPID=0' in unit and 'ExecMainStatus=1' in unit and 'InvocationID=13fb6f9159bb45589a7e332ce9eb7805' in unit
assert 'full campaign is not closed and successful' in (out/'full-terminal-refusal.log').read_text()
result=dict(failure_evidence_verified=True,full_accepted=False,actual_sdk_exit=1,driver_absent=True,supervisor_absent=True,source_inputs=len(state['source_inputs']),binary_sha256=state['binary_sha256'],native_files=len(actual),native_bytes=state['native_bytes'],wall_seconds=(state['end_ns']-state['start_ns'])/1e9,failure='start outcome unknown: input: context deadline exceeded',enclosing_20m_deadline_reached=False,cause_confirmed=False,acceptance_refusal_actual_exit=1)
(out/'full-failure-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
