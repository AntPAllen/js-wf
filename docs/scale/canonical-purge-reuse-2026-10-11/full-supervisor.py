import hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path(sys.argv[1]);revision=sys.argv[2]
root.mkdir() # unique campaign only; never restart an old store
inputs=json.loads((out/'source-inputs.json').read_text())
frozen=root/'source';frozen.mkdir()
for name,sha in inputs.items():
 raw=subprocess.check_output(['git','show',revision+':'+name]);assert hashlib.sha256(raw).hexdigest()==sha,name
 path=frozen/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
r=json.loads((out/'normal10-final-command.json').read_text())
selected=[e for e in r['executions'].values() if r['start_ns']<=e['start_ns']<r['end_ns'] and Path(e['argv'][0]).name=='integration.test']
assert len(selected)==1
e=selected[0]
binary=root/'integration.test';shutil.copy2(e['retained_binary'],binary)
assert hashlib.sha256(binary.read_bytes()).hexdigest()==e['sha256']
native=root/'native'
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',WF_GRAPH_PURGE_REUSE_SCALE='1',WF_GRAPH_PURGE_REUSE_NATIVE_ROOT=str(native),WF_PURGE_REUSE_ACTIVE='1')
for key in ('WF_PURGE_REUSE_COUNT','WF_PURGE_REUSE_POSTGRES','WF_PURGE_REUSE_SCALE'):env.pop(key,None)
command=[str(binary),'-test.run=^TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs$','-test.count=1','-test.v=true','-test.timeout=22m']
state=dict(revision=revision,source_inputs=inputs,binary_sha256=e['sha256'],binary=str(binary),command=command,supervisor_pid=os.getpid(),start_ns=time.time_ns(),finished=False,actual_exit=None,native_root=str(native),snapshot=str(frozen),environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_GRAPH_PURGE_REUSE_SCALE','WF_GRAPH_PURGE_REUSE_NATIVE_ROOT','WF_PURGE_REUSE_ACTIVE')})
def save():
 tmp=root/'state.json.tmp';tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(root/'state.json')
with (root/'full.log').open('w') as log:
 p=subprocess.Popen(command,cwd=frozen,env=env,stdout=log,stderr=subprocess.STDOUT)
 state['driver_pid']=p.pid;state['driver_proc_start']=Path('/proc/'+str(p.pid)+'/stat').read_text().split()[21];save()
 code=p.wait()
state['actual_exit']=code;state['end_ns']=time.time_ns();save()
for name,sha in inputs.items():assert hashlib.sha256((frozen/name).read_bytes()).hexdigest()==sha,name
census={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(native.rglob('*')) if p.is_file()}
(root/'native-census.json').write_text(json.dumps(census,indent=2)+'\n')
state['native_files']=len(census);state['native_bytes']=sum(v['bytes'] for v in census.values());state['finished']=True;save()
sys.exit(code)
