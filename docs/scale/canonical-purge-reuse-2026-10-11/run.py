import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-canonical-purge-reuse-20261011')
root.mkdir(exist_ok=True)
wrapper=root/'capture.py'
wrapper.write_bytes((out/'capture.py').read_bytes())
inputs=json.loads((out/'source-inputs.json').read_text())
def check():
    for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
jobs=[('normal10-final',False,True),('race10-final',True,True),('legacy10-final',False,False)]
for name,race,graph in jobs:
    check()
    native=root/(name+'-native')
    env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_PURGE_REUSE_COUNT='10')
    if graph:env.update(WF_GRAPH_PURGE_REUSE_SCALE='1',WF_GRAPH_PURGE_REUSE_NATIVE_ROOT=str(native))
    else:env.update(WF_PURGE_REUSE_SCALE='1')
    test=('TestCanonicalGraph' if graph else 'Test')+'TenThousandConcurrentPurgesAndThousandReusedIDs'
    command=['go','test',*(['-race'] if race else []),'-p=1','-exec','python3 '+str(wrapper),'./integration','-run','^'+test+'$','-count=1','-v','-timeout=22m']
    start=time.time_ns()
    with (out/(name+'.log')).open('w') as log:code=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
    check()
    executions={p.name:json.loads(p.read_text()) for p in root.glob('*-execution.json')}
    census={str(p.relative_to(native)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(native.rglob('*')) if p.is_file()}
    (out/(name+'-census.json')).write_text(json.dumps(census,indent=2)+'\n')
    (out/(name+'-command.json')).write_text(json.dumps(dict(command=command,environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_PURGE_REUSE_COUNT','WF_GRAPH_PURGE_REUSE_SCALE','WF_GRAPH_PURGE_REUSE_NATIVE_ROOT','WF_PURGE_REUSE_SCALE') if k in env},actual_exit=code,start_ns=start,end_ns=time.time_ns(),source_inputs_unchanged=True,executions=executions,native_root=str(native) if graph else None),indent=2)+'\n')
    print(name,'actual_exit',code,'native bytes',sum(v['bytes'] for v in census.values()),flush=True)
    if code:raise SystemExit(code)
