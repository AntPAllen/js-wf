import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2];root=Path('/home/exedev/js-wf-signal-dedup-model-20261011')
names=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()+['client/graph_signal_dedup_expiry_test.go']
names=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum','.github/workflows/graph-publication.yml')]
inputs={n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in sorted(set(names))}
(out/'model-source-inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
def check():return all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in inputs.items())
for mode in ('normal','race'):
 before=set(root.glob('*-execution.json'));start=time.time_ns()
 cmd=['go','test']+(['-race'] if mode=='race' else [])+['-p=1','-exec',f'python3 {root}/capture.py','./client','-run','^(TestCanonicalSignalMutationErrorPhases|TestCanonicalSignalRecoveryAfterDedupExpiry)$','-count=1','-v','-timeout=2m']
 with (out/('model-'+mode+'.log')).open('w') as f:
  p=subprocess.Popen(cmd,cwd=repo,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=f,stderr=subprocess.STDOUT);code=p.wait()
 end=time.time_ns()
 receipt=dict(argv=cmd,pid=p.pid,start_ns=start,end_ns=end,actual_exit=code,source_unchanged=check(),executions={x.name:json.loads(x.read_text()) for x in set(root.glob('*-execution.json'))-before})
 (out/('model-'+mode+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n')
 assert code==0 and check() and len(receipt['executions'])==1
