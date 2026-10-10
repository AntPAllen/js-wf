"""Run one bounded native port diagnostic; this is not production-cap qualification."""
import datetime, hashlib, json, os, subprocess, sys
from pathlib import Path
base=Path(__file__).resolve().parent
source=sys.argv[1]
checkout=Path('/home/exedev/js-wf-graph-limit-port-profile-qualification')
root=Path('/home/exedev/js-wf-graph-limit-port-profile-20261010')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
assert not root.exists()
root.mkdir()
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
state=dict(source=source,checkout=str(checkout),root=str(root),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),commands=[],accepted=False)
def save():(base/'state.json').write_text(json.dumps(state,indent=2)+'\n')
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()
names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return dict(source=source,files={n:hashlib.sha256((checkout/n).read_bytes()).hexdigest() for n in names})
(root/'source-before.json').write_text(json.dumps(inventory(),indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='1536MiB',WF_GRAPH_CONTINUATION_LIMIT_BUDGET='64',WF_GRAPH_LIMIT_PORT_PROFILE='1',GOTMPDIR=str(root/'go-tmp'),TMPDIR=str(root/'native-tmp'))
for name in ('go-tmp','native-tmp'):(root/name).mkdir()
state['environment']={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_GRAPH_CONTINUATION_LIMIT_BUDGET','WF_GRAPH_LIMIT_PORT_PROFILE','GOTMPDIR','TMPDIR')}
save()
def run(args,filename,cwd):
 row=dict(command=args,cwd=str(cwd),started=stamp());state['commands'].append(row);save()
 with (root/filename).open('wb') as output:row['exit']=subprocess.call(args,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT)
 row['finished']=stamp();save()
 return row['exit']
state['phase']='compile';save()
binary=root/'worker-race.test'
exit_code=run(['go','test','-race','-c','-o',str(binary),'./worker'],'compile.log',checkout)
if exit_code==0:
 state['binary']=dict(bytes=binary.stat().st_size,sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True))
 state['phase']='bounded_port_diagnostic';save()
 exit_code=run(['go','tool','test2json','-t','-p','js-wf/worker',str(binary),'-test.v=test2json','-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R1/archive=true$','-test.count=1','-test.timeout=5m'],'events.jsonl',checkout/'worker')
(root/'source-after.json').write_text(json.dumps(inventory(),indent=2)+'\n')
state.update(phase='closed',finished=stamp(),exit=exit_code);save()
sys.exit(exit_code)
