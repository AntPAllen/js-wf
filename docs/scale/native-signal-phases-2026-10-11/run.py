import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
repo=out.parents[2]
root=Path('/home/exedev/js-wf-native-signal-phases-20261011')
native=root/'native-active10-race'
assert not native.exists()
native.mkdir()
inputs=json.loads((out/'source-inputs.json').read_text())
def check(): return all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in inputs.items())
assert check()
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_GRAPH_PURGE_REUSE_SCALE='1',WF_GRAPH_PURGE_REUSE_NATIVE_ROOT=str(native),WF_PURGE_REUSE_ACTIVE='1',WF_PURGE_REUSE_COUNT='10')
for name in ('WF_PURGE_REUSE_PG','WF_PURGE_REUSE_SCALE'): env.pop(name,None)
cmd=['go','test','-race','-p=1','-exec',f'python3 {root}/capture.py','./integration','-run','^TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs$','-count=1','-v','-timeout=22m']
start=time.time_ns()
with (out/'native-active10-race.log').open('w') as f:
 p=subprocess.Popen(cmd,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT)
 code=p.wait()
end=time.time_ns()
files={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in native.rglob('*') if p.is_file()}
(out/'native-census.json').write_text(json.dumps(files,indent=2)+'\n')
receipt=dict(command=cmd,source_revision=json.loads((out/'source.json').read_text())['revision'],start_ns=start,end_ns=end,pid=p.pid,actual_exit=code,source_unchanged=check(),environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_GRAPH_PURGE_REUSE_SCALE','WF_GRAPH_PURGE_REUSE_NATIVE_ROOT','WF_PURGE_REUSE_ACTIVE','WF_PURGE_REUSE_COUNT')},executions={x.name:json.loads(x.read_text()) for x in root.glob('*-execution.json')},native_root=str(native),native_files=len(files),native_bytes=sum(v['bytes'] for v in files.values()))
(out/'native-command.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert check()
print(json.dumps(dict(actual_exit=code,native_files=len(files),native_bytes=receipt['native_bytes'])))
