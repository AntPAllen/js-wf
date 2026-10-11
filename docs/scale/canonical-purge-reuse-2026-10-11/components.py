import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-canonical-purge-reuse-20261011')
inputs=json.loads((out/'source-inputs.json').read_text())
def check():
 for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
for race in (False,True):
 check();mode='raw-'+('race' if race else 'normal')+'-final'
 command=['go','test',*(['-race'] if race else []),'-p=1','-exec','python3 '+str(root/'capture.py'),'./integrity','-run','^TestRawGraph','-count=1','-v','-timeout=3m']
 start=time.time_ns()
 with (out/(mode+'.log')).open('w') as log:code=subprocess.run(command,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=log,stderr=subprocess.STDOUT).returncode
 check()
 executions={p.name:json.loads(p.read_text()) for p in root.glob('*-execution.json')}
 (out/(mode+'-command.json')).write_text(json.dumps(dict(command=command,actual_exit=code,start_ns=start,end_ns=time.time_ns(),source_inputs_unchanged=True,executions=executions),indent=2)+'\n')
 print(mode,code,flush=True)
 if code:raise SystemExit(code)
