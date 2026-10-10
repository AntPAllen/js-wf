"""Qualify unambiguous journal headers against frozen SDK/native exports."""
import datetime,hashlib,json,os,subprocess,sys
from pathlib import Path
base=Path(__file__).resolve().parent;repo=base.parents[2]
checkout=Path('/home/exedev/js-wf-replay-shared-journal-header-qualification')
root=Path('/home/exedev/js-wf-replay-shared-journal-header-20261010')
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
for package,name,race in (('./wf','wf-race.test',True),('./sim','sim-normal.test',False),('./cmd/wf','cli-race.test',True),('./worker','worker-race.test',True)):
 run(['go','test']+(['-race'] if race else [])+['-c','-o',str(root/name),package],'compile-'+name+'.log')
 args=['go','tool','test2json','-t','-p','js-wf/'+package[2:],str(root/name),'-test.v=test2json','-test.count=1','-test.timeout=10m']
 if package=='./sim':args+=['-test.run=^TestPinnedRegressionCorpus$']
 if package=='./worker':args+=['-test.run=^(TestGraphReplaySnapshotOwnsInputsAndRejectedSignals|TestGraphReplaySnapshotRangeAndSlowAcquisition)$']
 if package=='./cmd/wf':args+=['-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin|TestReplayDeclaredGraphBeforePlugin|TestReplayInputBindingBeforePlugin|TestReplayRecordOrderBeforePlugin|TestReplayEpochWorkerBeforePlugin|TestReplayBundleEnvelope|TestReplayJournalHeaderAmbiguity|TestNativeGraphCursorOperatorHistory)$']
 run(args,package[2:].replace('/','-')+'-events.jsonl',checkout/package[2:])
production=checkout/'wf/replay_graph_format.go'
mutant=root/'validator-disabled.go';body=production.read_text()
needle='func validateReplayInputBinding(records []journal.Record, format, inputHash string) error {'
assert body.count(needle)==1
mutant.write_text(body.replace(needle,needle+'\n return nil // required negative control'))
overlay=root/'overlay.json';overlay.write_text(json.dumps(dict(Replace={str(production):str(mutant)})))
run(['go','test','-race','-overlay='+str(overlay),'./wf','-run','^TestReplayInputBindsStarted$/(graph_rehashed_input|unversioned_annotated_changed)$','-count=1'],'validator-disabled.log',expected=1)
run(['go','build','-race','-o',str(root/'wf'),'./cmd/wf'],'compile-cli.log')
run(['go','build','-race','-buildmode=plugin','-o',str(root/'handler.so'),'./worker/testdata/graphreplayplugin'],'compile-plugin.log')
order=checkout/'wf/replay_record_order.go'
order_needle='func validateReplayRecordOrder(records []journal.Record, invocation uint64) error {'
assert order.read_text().count(order_needle)==1
order_mutant=root/'record-order-disabled.go';order_mutant.write_text(order.read_text().replace(order_needle,order_needle+'\n return nil // required negative control'))
order_overlay=root/'record-order-overlay.json';order_overlay.write_text(json.dumps(dict(Replace={str(order):str(order_mutant)})))
run(['go','test','-race','-v','-overlay='+str(order_overlay),'./wf','./cmd/wf','-run','^TestReplayRecordOrderBefore(Handler|Plugin)$','-count=1','-timeout=5m'],'record-order-disabled.log',expected=1)
ownership_needle='if record.Epoch != 0 && record.WorkerID != "" {'
assert order.read_text().count(ownership_needle)==1
ownership_mutant=root/'epoch-worker-disabled.go';ownership_mutant.write_text(order.read_text().replace(ownership_needle,'if false && record.Epoch != 0 && record.WorkerID != "" {'))
ownership_overlay=root/'epoch-worker-overlay.json';ownership_overlay.write_text(json.dumps(dict(Replace={str(order):str(ownership_mutant)})))
run(['go','test','-race','-v','-overlay='+str(ownership_overlay),'./wf','./cmd/wf','-run','^TestReplayEpochWorker','-count=1','-timeout=5m'],'epoch-worker-disabled.log',expected=1)
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
 original=json.loads(path.read_text());child=isinstance(json.loads(__import__('base64').b64decode(original['input'])),dict)
 symbol='ChildWorkflow' if child else 'Workflow'
 expected='offline replay object is missing' if path.name.startswith('missing-') else 'completed' if path.name=='completed.json' else 'suspended'
 for mode in ('unversioned','graph-v1'):
  input_path=path
  if mode=='graph-v1':
   original['format']=mode;input_path=root/'formatted'/path.relative_to(native);input_path.parent.mkdir(parents=True,exist_ok=True);input_path.write_text(json.dumps(original)+'\n')
  replay(input_path,root/'wf',root/'handler.so',symbol,expected,mode+'-native-'+path.name)
  state['replays'][-1]['native_input']=str(path);state['replays'][-1]['native_input_sha256']=hash(path);save()
  if path.name=='completed.json':
   replay(input_path,root/'wf',root/'handler.so','MissingChildContinuation' if child else 'MissingContinuation','continuation stage is not registered',mode+'-native-missing-stage')
   state['replays'][-1]['native_input']=str(path);state['replays'][-1]['native_input_sha256']=hash(path);save()
# Actual simple native two-checkpoint input: preserve unknown/unversioned modes
# and all missing-marker variants, without rewriting producer receipts.
import copy
control=json.loads((native/'offline-2667111011/completed.json').read_text())
variants=root/'variants';variants.mkdir()
variant_symbol='ChildWorkflow' if isinstance(json.loads(__import__('base64').b64decode(control['input'])),dict) else 'Workflow'
for name in ('control','missing-canonical','all-markers-removed','checkpoint-metadata-removed','wrong-queue-index','missing-token','unknown-format'):
 bundle=copy.deepcopy(control)
 for record in bundle['journal']:
  event=record.get('payload')
  if not isinstance(event,dict):continue
  if record['kind']=='SignalConsumed':
   if name in ('missing-canonical','all-markers-removed'):event.pop('canonical_signal',None)
   if name=='all-markers-removed':event.pop('graph_child',None)
   if name=='wrong-queue-index':event['canonical_signal']['index']+=1
   if name=='missing-token':event['canonical_signal'].pop('token',None)
  if name in ('all-markers-removed','checkpoint-metadata-removed'):
   event.pop('checkpoint_metadata_ref',None);event.pop('checkpoint_metadata_hash',None)
 if name!='unknown-format':
  path=variants/(name+'-unversioned.json');path.write_text(json.dumps(bundle)+'\n')
  replay(path,root/'wf',root/'handler.so',variant_symbol,'completed','variant-unversioned-'+name)
 bundle['format']='graph-v99' if name=='unknown-format' else 'graph-v1'
 path=variants/(name+'-graph.json');path.write_text(json.dumps(bundle)+'\n')
 expected='completed' if name=='control' else 'unsupported offline replay format' if name=='unknown-format' else 'invalid step protocol in journal'
 replay(path,root/'wf',root/'handler.so',variant_symbol,expected,'variant-graph-'+name)
diagnostic=json.loads((base.parent/'graph-replay-input-binding-2026-10-10/diagnostic/report.json').read_text())
for row in diagnostic['rows']:
 path=Path(row['input']);assert hash(path)==row['input_sha256']
 replay(path,root/'wf',root/'handler.so','ChildWorkflow','completed' if row['name']=='control' else 'offline replay input differs from journal start','input-fixed-'+row['name'])
 replay(path,Path(diagnostic['cli']),Path(diagnostic['plugin']),'ChildWorkflow','completed','input-old-'+row['name'])
header=checkout/'cmd/wf/replay.go'
body=header.read_text()
import re
mutant_body,n=re.subn(r'Journal\s+\[\]journalwire\.Record', 'Journal json.RawMessage',body)
assert n==1
header_mutant=root/'journal-header-disabled.go';header_mutant.write_text(mutant_body)
header_overlay=root/'header-overlay.json';header_overlay.write_text(json.dumps(dict(Replace={str(header):str(header_mutant)})))
run(['go','test','-race','-overlay='+str(header_overlay),'./cmd/wf','-run','^TestReplayJournalHeaderAmbiguity$/(duplicate_|alias_|escaped_)','-count=1'],'journal-header-disabled.log',expected=1)
wire=checkout/'internal/journalwire/record.go'
wire_body=wire.read_text();assert wire_body.count('checkpoint.DecodeUnambiguous(raw, &wire)')==1
wire_mutant=root/'ordinary-decode.go';wire_mutant.write_text(wire_body.replace('checkpoint.DecodeUnambiguous(raw, &wire)','json.Unmarshal(raw, &wire)').replace('"js-wf/internal/checkpoint"','_ "js-wf/internal/checkpoint"'))
wire_overlay=root/'wire-overlay.json';wire_overlay.write_text(json.dumps(dict(Replace={str(wire):str(wire_mutant)})))
run(['go','test','-race','-overlay='+str(wire_overlay),'./wf','-run','^TestReplayJournalHeadersBeforeHandler$/(duplicate_|alias_|escaped_|unknown_header)','-count=1'],'ordinary-decode.log',expected=1)
(root/'source-after.json').write_text(json.dumps(inventory(),indent=2)+'\n')
state['binaries']={name:dict(bytes=(root/name).stat().st_size,sha256=hash(root/name)) for name in ('wf-race.test','sim-normal.test','cli-race.test','worker-race.test','wf','handler.so')}
state.update(phase='closed',exit=0,finished=stamp());save()
