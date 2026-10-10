"""Frozen canonical running cancellation poll, race plus extended normal seeds."""
import datetime,hashlib,json,os,subprocess,sys
from pathlib import Path
base=Path(__file__).resolve().parent;checkout=Path('/home/exedev/js-wf-graph-cancel-poll-qualification');root=Path('/home/exedev/js-wf-graph-cancel-poll-20261010');source=sys.argv[1]
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
assert not root.exists();root.mkdir()
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def hash(p):return hashlib.sha256(p.read_bytes()).hexdigest()
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines();names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return dict(source=source,files={n:hash(checkout/n) for n in names})
(root/'source-before.json').write_text(json.dumps(inventory(),indent=2)+'\n')
s=dict(source=source,checkout=str(checkout),root=str(root),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),commands=[],accepted=False)
def save():(base/'state.json').write_text(json.dumps(s,indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='768MiB',SIM_COVERAGE_SUMMARY='1',SIM_PROGRESS='1',FAULT_TRACE_OUT=str(root/'failure.json'));save()
def run(args,output,cwd=checkout,expected=0,seeds='1000'):
 row=dict(command=args,cwd=str(cwd),started=stamp(),expected=expected,seeds=seeds);s['commands'].append(row);save()
 with (root/output).open('wb') as f:row['exit']=subprocess.call(args,cwd=cwd,env=dict(env,SIM_SEEDS=seeds),stdout=f,stderr=subprocess.STDOUT)
 row['finished']=stamp();save();assert row['exit']==expected
for mode in ('race','normal'):run(['go','test']+(['-race'] if mode=='race' else [])+['-c','-o',str(root/('sim-'+mode+'.test')),'./sim'],'compile-'+mode+'.log')
def test(mode,selector,output,seeds='1000'):
 run(['go','tool','test2json','-t','-p','js-wf/sim',str(root/('sim-'+mode+'.test')),'-test.v=test2json','-test.run='+selector,'-test.count=1','-test.timeout=120m'],output,checkout/'sim',seeds=seeds)
test('race','^TestSeededGraphRunningCancelPollReplay$','race-events.jsonl')
test('normal','^TestPinnedRegressionCorpus$','corpus-events.jsonl')
p=checkout/'worker/worker.go';needle='func (w *Worker) pollRunningCancellation(ctx context.Context, typ, id, generation string, expected *cancelWaiter) (bool, error) {\n\tif w.graphJournal != nil && w.graphJournal.CanonicalSignals() {'
assert p.read_text().count(needle)==1
mutant=root/'disabled.go';mutant.write_text(p.read_text().replace(needle,needle.replace('if w.graphJournal','if false && w.graphJournal')))
overlay=root/'overlay.json';overlay.write_text(json.dumps(dict(Replace={str(p):str(mutant)})))
run(['go','test','-race','-v','-overlay='+str(overlay),'./sim','-run','^TestPinnedRegressionCorpus$/graph-running-cancel-poll-(bound_source_purged|root_unknown|owned_read_unknown).json$','-count=1','-timeout=5m'],'negative.log',expected=1)
test('normal','^TestSeededGraphRunningCancelPollReplay$','normal100000-events.jsonl','100000')
(root/'source-after.json').write_text(json.dumps(inventory(),indent=2)+'\n')
s.update(phase='closed',exit=0,finished=stamp(),binaries={n:dict(bytes=(root/n).stat().st_size,sha256=hash(root/n)) for n in ('sim-race.test','sim-normal.test')});save()
