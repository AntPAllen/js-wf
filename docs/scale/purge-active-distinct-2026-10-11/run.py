import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent;checkout=Path('/home/exedev/js-wf-purge-active-unique-20261011');root=Path('/home/exedev/js-wf-purge-active-distinct-20261011')
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()+['integration/purge_reuse_active_test.go']
names=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum','.github/workflows/canonical-purge-reuse.yml')]
inputs={n:hashlib.sha256((checkout/n).read_bytes()).hexdigest() for n in sorted(set(names))}
(out/'source-inputs.json').write_text(json.dumps(dict(checkout=str(checkout),base_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip(),files=inputs),indent=2)+'\n')
(out/'fixture.patch').write_bytes(subprocess.check_output(['git','diff','--','integration/purge_reuse_scale_test.go','.github/workflows/canonical-purge-reuse.yml'],cwd=checkout))
(out/'active-helper.go.txt').write_bytes((checkout/'integration/purge_reuse_active_test.go').read_bytes())
def check():return all(hashlib.sha256((checkout/n).read_bytes()).hexdigest()==h for n,h in inputs.items())
for mode in ('normal','race'):
 before=set(root.glob('*-execution.json'));start=time.time_ns()
 cmd=['go','test']+(['-race'] if mode=='race' else [])+['-p=1','-exec',f'python3 {root}/capture.py','./integration','-run','^TestPurgeScaleActiveHandlers','-count=1','-v','-timeout=2m']
 with (out/(mode+'.log')).open('w') as f:
  p=subprocess.Popen(cmd,cwd=checkout,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=f,stderr=subprocess.STDOUT);code=p.wait()
 end=time.time_ns()
 receipt=dict(argv=cmd,pid=p.pid,start_ns=start,end_ns=end,actual_exit=code,source_unchanged=check(),executions={x.name:json.loads(x.read_text()) for x in set(root.glob('*-execution.json'))-before})
 (out/(mode+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n');assert code==0 and check() and len(receipt['executions'])==1
