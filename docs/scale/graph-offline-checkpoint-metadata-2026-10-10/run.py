"""Qualify checkpoint metadata against frozen SDK and retained native exports."""
import datetime,hashlib,json,os,subprocess,sys
from pathlib import Path
base=Path(__file__).resolve().parent;repo=base.parents[2]
checkout=Path('/home/exedev/js-wf-checkpoint-metadata-qualification')
root=Path('/home/exedev/js-wf-checkpoint-metadata-20261010')
source=sys.argv[1]
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
assert not root.exists();root.mkdir()
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()
state=dict(source=source,checkout=str(checkout),root=str(root),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),commands=[],replays=[],accepted=False)
def save():(base/'state.json').write_text(json.dumps(state,indent=2)+'\n')
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()
names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return dict(source=source,files={n:hash(checkout/n) for n in names})
(root/'source-before.json').write_text(json.dumps(inventory(),indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='768MiB')
state['environment']={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT')};save()
def run(args,filename,cwd=checkout,expected=0):
 row=dict(command=args,cwd=str(cwd),started=stamp(),expected_exit=expected);state['commands'].append(row);save()
 with (root/filename).open('wb') as output:row['exit']=subprocess.call(args,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT)
 row['finished']=stamp();save();assert row['exit']==expected
for package,name,race in (('./wf','wf-race.test',True),('./sim','sim-normal.test',False),('./cmd/wf','cli-race.test',True)):
 run(['go','test']+(['-race'] if race else [])+['-c','-o',str(root/name),package],'compile-'+name+'.log')
 args=['go','tool','test2json','-t','-p','js-wf/'+package[2:],str(root/name),'-test.v=test2json','-test.count=1','-test.timeout=10m']
 if package=='./sim':args+=['-test.run=^TestPinnedRegressionCorpus$']
 if package=='./cmd/wf':args+=['-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin)$']
 run(args,package[2:].replace('/','-')+'-events.jsonl',checkout/package[2:])
production=checkout/'wf/replay_graph_checkpoint.go'
mutant=root/'validator-disabled.go';body=production.read_text()
needle='func ValidateReplayGraphCheckpoints(records []journal.Record, objects map[string][]byte, typ, id string, invocation uint64) error {'
assert body.count(needle)==1
mutant.write_text(body.replace(needle,needle+'\n return nil // required negative control'))
overlay=root/'overlay.json';overlay.write_text(json.dumps(dict(Replace={str(production):str(mutant)})))
run(['go','test','-race','-overlay='+str(overlay),'./wf','-run','^TestReplayGraphCheckpointMetadata$/buffered=false/(missing_metadata|wrong_anchor)$','-count=1'],'validator-disabled.log',expected=1)
run(['go','build','-race','-o',str(root/'wf'),'./cmd/wf'],'compile-cli.log')
run(['go','build','-race','-buildmode=plugin','-o',str(root/'handler.so'),'./worker/testdata/graphreplayplugin'],'compile-plugin.log')
marker=root/'effect'
def replay(path,cli,plugin,symbol,expected,label):
 args=[str(cli),'-url','nats://127.0.0.1:1','-handler-plugin',str(plugin),'-handler-symbol',symbol,'-replay-bundle',str(path),'replay']
 result=subprocess.run(args,env=dict(env,WF_REPLAY_EFFECT_MARKER=str(marker)),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=20)
 row=dict(label=label,input=str(path),input_sha256=hash(path),command=args,exit=result.returncode,output=result.stdout.decode(),expected=expected);state['replays'].append(row);save()
 if expected in ('completed','suspended'):
  report=json.loads(result.stdout);assert result.returncode==0 and report['status']==expected
  if expected=='completed':assert report['result']==60
 else:assert result.returncode!=0 and expected in row['output']
 assert not marker.exists()
native=Path('/home/exedev/js-wf-child-offline-20261010/artifacts')
producer=repo/'docs/scale/graph-continuation-child-offline-2026-10-10'
review=json.loads((producer/'review.json').read_text());assert review['accepted'] and review['git_verified_inputs']==3312
state['producer_source']=review['source'];state['producer_review_sha256']=hash(producer/'review.json');save()
exports=sorted(p for p in native.rglob('*.json') if p.name in ('completed.json','suspended.json','child-pending.json','missing-objects.json','missing-child-result.json'));assert len(exports)==76
for path in exports:
 assert hash(path)==hash(producer/'qualified/artifacts'/path.relative_to(native))
 bundle=json.loads(path.read_text());child=isinstance(json.loads(__import__('base64').b64decode(bundle['input'])),dict)
 symbol='ChildWorkflow' if child else 'Workflow'
 expected='offline replay object is missing' if path.name.startswith('missing-') else 'completed' if path.name=='completed.json' else 'suspended'
 replay(path,root/'wf',root/'handler.so',symbol,expected,'native-'+path.name)
 if path.name=='completed.json':replay(path,root/'wf',root/'handler.so','MissingChildContinuation' if child else 'MissingContinuation','continuation stage is not registered','native-missing-stage')
diag=Path('/home/exedev/js-wf-offline-checkpoint-metadata-diagnostic-20261010')
old=json.loads((base/'old-diagnostic.json').read_text())
for row in old['rows']:
 path=diag/(row['name']+'.json');assert hash(path)==row['input_sha256']
 expected='completed' if row['name']=='control' else 'offline replay object is missing' if row['name']=='missing-metadata' else 'invalid step protocol in journal'
 replay(path,root/'wf',root/'handler.so',old['symbol'],expected,'fixed-'+row['name'])
 replay(path,Path(old['cli']),Path(old['plugin']),old['symbol'],'completed','old-'+row['name'])
(root/'source-after.json').write_text(json.dumps(inventory(),indent=2)+'\n')
state['binaries']={name:dict(bytes=(root/name).stat().st_size,sha256=hash(root/name)) for name in ('wf-race.test','sim-normal.test','cli-race.test','wf','handler.so')}
state.update(phase='closed',exit=0,finished=stamp());save()
