import hashlib, json, os, subprocess, time
from pathlib import Path
repo=Path(__file__).resolve().parents[3]
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-fetch-expired-option-20261011')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
paths=[p for p in repo.rglob('*.go') if '.git' not in p.parts]
paths += [repo/'go.mod',repo/'go.sum']
inputs={str(p.relative_to(repo)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(paths)}
(out/'source-inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
steps=[]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
for mode in ['normal','race']:
 binary=root/(mode+'.test')
 build=['go','test','-c','-o',str(binary)] + (['-race'] if mode=='race' else []) + ['./worker']
 for label,cmd in [('build',build),('test',[str(binary),'-test.run=^(TestDispatchFetch|TestPartitionCancellationDuringStalledBatch)','-test.count=1','-test.timeout=30s','-test.v'])]:
  start=time.time_ns()
  with (out/(mode+'-'+label+'.log')).open('w') as f:
   p=subprocess.Popen(cmd,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT)
   if label=='test': executable={'sha256':sha(Path('/proc')/str(p.pid)/'exe'),'bytes':binary.stat().st_size}
   code=p.wait()
  receipt=dict(mode=mode,label=label,argv=cmd,pid=p.pid,start_ns=start,end_ns=time.time_ns(),actual_exit=code)
  if label=='test':receipt['executable']=executable
  steps.append(receipt)
  (out/'commands.json').write_text(json.dumps(steps,indent=2)+'\n')
  if code:raise SystemExit(code)
assert all(sha(repo/name)==v['sha256'] for name,v in inputs.items())
print('normal/race terminal, inputs unchanged')
