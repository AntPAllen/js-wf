import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-native-expired-pull-fix-20261011')
source=root/'source';binary=root/'integration-race.test';native=root/'native-active10-race'
assert not native.exists();native.mkdir()
inputs=json.loads((out/'source-inputs.json').read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def check():return all(sha(source/n)==h for n,h in inputs.items())
assert check()
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_GRAPH_PURGE_REUSE_SCALE='1',WF_GRAPH_PURGE_REUSE_NATIVE_ROOT=str(native),WF_PURGE_REUSE_ACTIVE='1',WF_PURGE_REUSE_COUNT='10')
for n in ['WF_PURGE_REUSE_POSTGRES','WF_PURGE_REUSE_SCALE']:env.pop(n,None)
steps=[]
for label,cmd in [('build',['go','test','-c','-race','-p=1','-o',str(binary),'./integration']),('test',[str(binary),'-test.run=^TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs$','-test.count=1','-test.v','-test.timeout=22m'])]:
 start=time.time_ns()
 with (out/(label+'.log')).open('w') as f:
  p=subprocess.Popen(cmd,cwd=source if label=='build' else source/'integration',env=env,stdout=f,stderr=subprocess.STDOUT)
  receipt=dict(label=label,argv=cmd,pid=p.pid,start_ns=start,environment={k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','WF_GRAPH_PURGE_REUSE_SCALE','WF_GRAPH_PURGE_REUSE_NATIVE_ROOT','WF_PURGE_REUSE_ACTIVE','WF_PURGE_REUSE_COUNT']})
  if label=='test':receipt.update(binary_sha256=sha(Path('/proc')/str(p.pid)/'exe'),binary_bytes=binary.stat().st_size)
  (out/'live.json').write_text(json.dumps(receipt,indent=2)+'\n')
  code=p.wait()
 receipt.update(actual_exit=code,end_ns=time.time_ns(),source_unchanged=check())
 steps.append(receipt);(out/'commands.json').write_text(json.dumps(steps,indent=2)+'\n')
 if code:break
files={str(p.relative_to(native)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(native.rglob('*')) if p.is_file()}
(out/'native-census.json').write_text(json.dumps(files,indent=2)+'\n')
assert check()
print(json.dumps({'actual_exit':code,'native_files':len(files),'native_bytes':sum(v['bytes'] for v in files.values())}))
raise SystemExit(code)
