"""Frozen native restored running-effect cancellation matrix."""
import datetime,hashlib,json,os,subprocess,sys
from pathlib import Path
base=Path(__file__).resolve().parent
checkout=Path('/home/exedev/js-wf-running-effect-cancel-qualification')
root=Path('/home/exedev/js-wf-running-effect-cancel-20261010')
source=sys.argv[1]
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
assert not root.exists();root.mkdir()
def stamp(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def hash(p): return hashlib.sha256(p.read_bytes()).hexdigest()
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()
names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory(): return dict(source=source,files={n:hash(checkout/n) for n in names})
(root/'source-before.json').write_text(json.dumps(inventory(),indent=2)+'\n')
s=dict(source=source,checkout=str(checkout),root=str(root),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),commands=[],accepted=False)
def save(): (base/'state.json').write_text(json.dumps(s,indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='768MiB',TMPDIR=str(root/'go-tmp'));Path(env['TMPDIR']).mkdir();save()
def run(args,output,cwd):
 row=dict(command=args,started=stamp());s['commands'].append(row);save()
 with (root/output).open('wb') as f:row['exit']=subprocess.call(args,cwd=cwd,env=env,stdout=f,stderr=subprocess.STDOUT)
 row['finished']=stamp();save();assert row['exit']==0
binary=root/'worker-race.test'
run(['go','test','-race','-c','-o',str(binary),'./worker'],'compile.log',checkout)
run(['go','tool','test2json','-t','-p','js-wf/worker',str(binary),'-test.v=test2json','-test.run=^TestNativeGraphContinuationCancelRunningEffect$','-test.count=1','-test.timeout=25m'],'events.jsonl',checkout/'worker')
(root/'source-after.json').write_text(json.dumps(inventory(),indent=2)+'\n')
s.update(phase='closed',exit=0,finished=stamp(),binary=dict(bytes=binary.stat().st_size,sha256=hash(binary)));save()
