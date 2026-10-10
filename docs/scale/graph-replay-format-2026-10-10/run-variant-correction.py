"""Correct variant handler selection; never repeat closed native/SDK commands."""
import base64,copy,datetime,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
original=json.loads((base/'state.json').read_text());root,checkout=Path(original['root']),Path(original['checkout'])
properties=subprocess.check_output(['systemctl','--user','show','js-wf-replay-format-20261010.service','-p','LoadState','-p','MainPID','-p','ExecMainStatus','-p','InvocationID'],text=True)
assert 'LoadState=loaded\n' in properties and 'MainPID=0\n' in properties and 'ExecMainStatus=1\n' in properties and 'InvocationID='+original['invocation']+'\n' in properties
assert len(original['replays'])==193 and original['replays'][-1]['label']=='variant-unversioned-control' and original['replays'][-1]['exit']==1
(root/'original-supervisor-exit.txt').write_text(properties)
def hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()
before=json.loads((root/'source-before.json').read_text())
assert all(hash(checkout/n)==digest for n,digest in before['files'].items())
(root/'source-after-failed-run.json').write_text(json.dumps(before,indent=2)+'\n')
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
state=dict(source=original['source'],root=str(root),checkout=str(checkout),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),original_supervisor_exit=1,original_variant_error='Workflow selected for actual child bundle; child_middle_v1 is correctly unavailable in the simple definition.',replays=[],binaries={n:dict(bytes=(root/n).stat().st_size,sha256=hash(root/n)) for n in ('wf-race.test','sim-normal.test','cli-race.test','worker-race.test','wf','handler.so')},accepted=False)
def save():(base/'variant-correction.json').write_text(json.dumps(state,indent=2)+'\n')
save()
control_path=Path('/home/exedev/js-wf-child-offline-20261010/artifacts/offline-2667111011/completed.json')
control=json.loads(control_path.read_text())
symbol='ChildWorkflow' if isinstance(json.loads(base64.b64decode(control['input'])),dict) else 'Workflow'
assert symbol=='ChildWorkflow'
state['selected_symbol']=symbol;state['control_input_sha256']=hash(control_path);save()
variants=root/'corrected-variants';variants.mkdir()
marker=root/'effect'
def replay(path,expected,label):
 args=[str(root/'wf'),'-url','nats://127.0.0.1:1','-handler-plugin',str(root/'handler.so'),'-handler-symbol',symbol,'-replay-bundle',str(path),'replay']
 result=subprocess.run(args,env=dict(os.environ,WF_REPLAY_EFFECT_MARKER=str(marker)),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=20)
 row=dict(label=label,input=str(path),input_sha256=hash(path),command=args,exit=result.returncode,output=result.stdout.decode(),expected=expected);state['replays'].append(row);save()
 if expected=='completed':assert result.returncode==0 and json.loads(result.stdout)['result']==60
 else:assert result.returncode!=0 and expected in row['output']
 assert not marker.exists()
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
  replay(path,'completed','variant-unversioned-'+name)
 bundle['format']='graph-v99' if name=='unknown-format' else 'graph-v1'
 path=variants/(name+'-graph.json');path.write_text(json.dumps(bundle)+'\n')
 expected='completed' if name=='control' else 'unsupported offline replay format' if name=='unknown-format' else 'invalid step protocol in journal'
 replay(path,expected,'variant-graph-'+name)
assert all(hash(checkout/n)==digest for n,digest in before['files'].items())
assert all(hash(root/n)==digest['sha256'] for n,digest in state['binaries'].items())
(root/'source-after-correction.json').write_text(json.dumps(before,indent=2)+'\n')
state.update(phase='closed',exit=0,finished=stamp());save()
