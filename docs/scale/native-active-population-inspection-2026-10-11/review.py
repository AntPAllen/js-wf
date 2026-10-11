import base64, hashlib, json, os
from pathlib import Path
out=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def census(root):return {str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(root.rglob('*')) if p.is_file()}
original=Path('/home/exedev/js-wf-native-append-commit-phases-20261011/native-active10-race')
assert census(original)==json.loads((out/'original-census.json').read_text())
for directory,expected in [(out,1),(out/'phases',0)]:
 inputs=json.loads((directory/'source-inputs.json').read_text())
 source=Path(inputs['source'])
 for name,digest in inputs['files'].items():assert sha(source/name)==digest,name
 assert sha(source/inputs['helper']['path'])==inputs['helper']['sha256']
 commands=json.loads((directory/'commands.json').read_text())
 assert commands[0]['actual_exit']==0 and commands[1]['actual_exit']==expected
 binary=Path(commands[1]['argv'][0])
 assert sha(binary)==commands[1]['binary_sha256']
 assert binary.stat().st_size==commands[1]['binary_bytes']
 assert not Path('/proc',str(commands[1]['pid'])).exists()
 copy=Path(json.loads((directory/'copy-verified.json').read_text())['copy'])
 target=directory/'closed-copy-census.json'
 if not target.exists(): target.write_text(json.dumps(census(copy),indent=2)+'\n')
 else:assert census(copy)==json.loads(target.read_text())
d=json.loads((out/'phases/observations.json').read_text())
assert not d['Error']
observed={o['Phase']:o for o in d['Observations']}
errors={key:o['Error'] for key,o in observed.items() if o['Error']}
assert set(errors)=={'metadata.round0.node0.PURGE_SCALE_AUTH','metadata.round0.node0.WF_INV'}
invocations=[]
for kind in ['old','live']:
 for i in range(10):
  ident=f'{kind}-{i:05d}'
  v=observed['invocation.'+ident]['Value']
  assert v['input']=={'kind':kind,'index':i}
  assert not v['start']['Pending'] and v['source']['Sequence']==v['start']['Invocation']
  assert v['tail']==v['status']['Tail']
  records=v['records'] or []
  if kind=='old':assert len(records)==2 and records[-1]['kind']=='Completed' and v['tail']==2
  elif i in [1,5]:assert not records and v['tail']==0
  else:assert len(records)==1 and records[0]['kind']=='Started' and v['tail']==1
  invocations.append({'id':ident,'partition':v['partition'],'tail':v['tail'],'last_kind':v['status']['Kind']})
messages=observed['retained.run.messages']['Value']['messages']
assert len(messages)==10 and [m['Sequence'] for m in messages]==list(range(11,21))
for m in messages:
 ident=base64.b64decode(m['Data']).decode().split('.')[1]
 v=observed['invocation.'+ident]['Value']
 assert m['Subject']=='wf.run.'+str(v['partition'])
for partition in [51,63]:
 c=observed[f'consumer.WF_P_{partition:02d}']['Value']
 assert c['delivered']['consumer_seq']==0 and c['num_pending']==1 and c['num_ack_pending']==0
 assert partition%6==3
assert all(v['partition']%6!=3 for v in invocations if v['id'].startswith('old-'))
assert len([k for k in observed if k.startswith('consumer.WF_P_')])==64
result={'original_unchanged':True,'files':len(census(original)),'bytes':sum(v['bytes'] for v in census(original).values()),'phase_probe_exit':0,'invocations':invocations,'retained_dispatch_messages':10,'metadata_errors':errors,'cause_confirmed':False}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print('closed-copy evidence verified; original unchanged; cause remains unconfirmed')
