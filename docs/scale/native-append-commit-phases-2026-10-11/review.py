"""Bind the actual diagnostic executable, source, native closure and outcome."""
import hashlib,io,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
repo=out.parents[2]
sha=lambda b:hashlib.sha256(b).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
for n,h in inputs.items(): assert sha((repo/n).read_bytes())==h,n
if not (out/'native-command.json').exists():
 observation=json.loads((out/'live-observation.json').read_text())
 pid=observation['sdk_pid']
 proc=Path('/proc/'+str(pid))
 if proc.exists() and sha((proc/'exe').read_bytes())==observation['sdk_sha256']:
  raise RuntimeError(f'native SDK {pid} is live; terminal acceptance refused')
 raise RuntimeError('native terminal receipt unavailable; acceptance refused')
receipt=json.loads((out/'native-command.json').read_text())
assert receipt['source_unchanged'] and receipt['actual_exit'] in (0,1)
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(receipt['source_revision']+':'+n+'\n' for n in inputs).encode()))
for n,h in inputs.items():
 header=stream.readline().split(); assert header[1]==b'blob'
 data=stream.read(int(header[2])); assert stream.read(1)==b'\n'
 assert sha(data)==h,n
assert len(receipt['executions'])==1
execution=next(iter(receipt['executions'].values()))
assert execution['actual_exit']==receipt['actual_exit']
assert receipt['start_ns']<=execution['start_ns']<execution['end_ns']<=receipt['end_ns']
assert not Path('/proc/'+str(execution['pid'])).exists() and not Path('/proc/'+str(receipt['pid'])).exists()
binary=Path(execution['retained_binary'])
assert binary.stat().st_size==execution['bytes'] and sha(binary.read_bytes())==execution['sha256']
assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert receipt['environment']['WF_PURGE_REUSE_COUNT']=='10' and receipt['environment']['WF_PURGE_REUSE_ACTIVE']=='1'
assert receipt['environment']['WF_GRAPH_PURGE_REUSE_SCALE']=='1'
log=(out/'native-active10-race.log').read_text()
assert 'count=10 reuse=1 active=true full_scale=false' in log
assert not re.search(r'^\s*--- SKIP:',log,re.M)
name='TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs'
report=None
if receipt['actual_exit']==0:
 assert f'--- PASS: {name} ' in log and not re.search(r'^\s*--- FAIL:',log,re.M)
 report=json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})',log)[1])
 assert all(report[k]==11 for k in ('Invocations','Journals','Terminal'))
 assert report['Entries']==66 and report['Retired']==report['RetiredTombstones']==9
 assert report['References']['ReaderPins']==0
else:
 assert f'--- FAIL: {name} ' in log
 assert 'canonical raw purge/reuse audit=' not in log
native=Path(receipt['native_root'])
actual={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=sha(p.read_bytes())) for p in native.rglob('*') if p.is_file()}
assert actual==json.loads((out/'native-census.json').read_text())
assert len(actual)==receipt['native_files'] and sum(v['bytes'] for v in actual.values())==receipt['native_bytes']
result=dict(closure_verified=True,actual_exit=receipt['actual_exit'],source=receipt['source_revision'],source_inputs=len(inputs),actual_sdk_sha256=execution['sha256'],sdk_seconds=(execution['end_ns']-execution['start_ns'])/1e9,native_files=len(actual),native_bytes=receipt['native_bytes'],raw_report=report,completed_old=('completed 10 old invocations in ' in log),active_population_complete=('kept 10 other handlers executing before purge' in log),purge_reuse_overlap=('purged 10 while 10 remained live; reused 1 ids' in log),full_scale_accepted=False,native_failure_cause_established=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
