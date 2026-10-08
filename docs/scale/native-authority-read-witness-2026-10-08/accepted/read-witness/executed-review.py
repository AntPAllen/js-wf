import sys,json,copy,base64,hashlib,re,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-native-authority-witness-20261008');out=repo/'docs/scale/native-authority-read-witness-2026-10-08/accepted/read-witness'
sys.path.insert(0,str(repo/'scripts'))
from worker_leaf_wire import protocol
revision=json.loads((root/'source-before.json').read_text())['revision']
paths=[p for p in (root/'blob-stores').rglob('object-proof.json') if json.loads(p.read_text())['scenario'].startswith(('authority-read-witness','read-witness-'))]
assert len(paths)==5

def check(d,identity):
 scenario=d['scenario'];replicas=d['replicas'];assert replicas in (1,3)
 peers=identity['peers'];assert len(peers)==len({x['id'] for x in peers})==replicas
 assert {x['name'] for x in peers}=={f'wf-test-{i}' for i in range(replicas)}
 assert all(x['version']=='2.15.0' and x['embedding_commit']==revision[:7] for x in peers)
 assert identity['parent_budget_seconds']==30
 if scenario=='authority-read-witness':
  assert d['parent_budget_seconds']==30 and d['logical_root_head_unchanged']==2
  assert d['absence_physical_sequence']>0 and d['first_logical_head']==1
  assert d['lost_acknowledgments']==1 and d['lost_response_rejected'] is True and d['canceled_before_get'] is True
  cases={x['mode']:x for x in d['cases']};assert len(cases)==7
  for mode in ['stale-value','stale-absence','future-value']:
   c=cases[mode];assert c['snapshot_reads']==2 and not c['rejected']
   assert c['returned_root']==c['expected_root'] and c['returned_root']['Head']==2 and c['returned_root']['Token']=='new'
   assert c['after_sequence']>c['before_sequence']>0
  c=cases['persistent-stale'];assert c['snapshot_reads']==16 and c['rejected'] is True and c['after_sequence']==c['before_sequence'] and c['returned_root']['Head']==0
  for mode in ['blob-missing-False','blob-missing-True']:
   # Go bool formatting is lowercase.
   c=cases[mode.lower().replace('blob-missing-','blob-missing-')];assert c['snapshot_reads']==2 and c['returned_blob']==c['expected_blob']
   assert c['returned_blob']['Revision']==2 and c['returned_blob']['Fence']['Generation']==1 and c['returned_blob']['Fence']['Phase']=='closed'
  c=cases['legacy-v1-upgrade'];assert c['root']['Head']==7 and c['root']['Token']=='legacy'
  assert c['envelope']['schema']=='js-wf-blob-authority-v2' and c['envelope']['revision']==7 and c['envelope']['root']==c['root'] and c['physical_sequence']>0
 elif scenario=='read-witness-replacement-race':
  assert d['old_witness_rejected'] is True and d['old_root']['Head']==1 and d['latest_root']['Head']==2 and d['latest_root']==d['returned_root']
  h=d['held'];f=d['forwarded'];packet=base64.b64decode(h['packet'],validate=True)
  subject='wf.blob.authority.root.'+hashlib.sha256(b'read-race').hexdigest()
  assert h['subject']==f['subject']==subject and h['forwarded_bytes']==0 and h['disposition']=='held'
  assert f['packet']==h['packet'] and f['disposition']=='forwarded' and f['forwarded_bytes']==len(packet)
  parsed=protocol(packet);assert len(parsed)==1;op,parts,body=parsed[0];assert op==b'HPUB' and parts[1].decode()==subject
  header=body[:int(parts[-2])];assert b'Wf-Authority-Read-Witness: 1\r\n' in header
  v=json.loads(body[int(parts[-2]):]);assert v['schema']=='js-wf-blob-authority-v2' and v['root']==d['old_root'] and v['revision']==1
  trace=d['wire'];assert not trace['truncated'] and len(trace['connections'])==1
  streams={k:bytearray() for k in ['client_to_server','server_to_client']}
  for frame in trace['frames']:assert frame['connection']==1;streams[frame['direction']].extend(base64.b64decode(frame['data']))
  assert bytes(streams['client_to_server']).count(packet)==1
  incoming=protocol(streams['server_to_client'],True);infos=[v for op,_,v in incoming if op==b'INFO'];assert infos
  for info in infos:
   ps=[p for p in peers if p['id']==info['server_id']];assert len(ps)==1
   assert ps[0]['name']==info['server_name'] and ps[0]['version']==info['version'] and ps[0]['embedding_commit']==info['git_commit']
  replies=[]
  for op,part,body in incoming:
   if op not in [b'MSG',b'HMSG'] or part[1]!=parts[2]:continue
   if op==b'HMSG':body=body[int(part[-2]):]
   replies.append(json.loads(body))
  assert len(replies)==1 and replies[0]['error']['err_code']==10071
 elif scenario=='read-witness-quorum-partition':
  assert replicas==3 and d['parent_budget_seconds']==30 and 0<=d['minority_node']<3
  assert d['isolated_reads_rejected'] is True and d['healed_head_preserved'] is True
  assert d['before_root']['Head']==1 and d['before_root']['Token']=='old'
  assert d['majority_root']['Head']==2 and d['majority_root']['Token']=='majority' and d['healed_root']==d['majority_root']
  assert d['minority_before_error'] and d['minority_after_error']
 else:raise AssertionError(scenario)

controls=[];proofs=[]
for p in paths:
 d=json.loads(p.read_text());identity=json.loads((p.parent/'object-fixture-identity.json').read_text()) if (p.parent/'object-fixture-identity.json').exists() else {'peers':d['peers'],'parent_budget_seconds':30}
 check(d,identity);proofs.append({'scenario':d['scenario'],'replicas':d['replicas']})
 changes=[('wrong_replicas',lambda x:x.update(replicas=2))]
 if d['scenario']=='authority-read-witness':
  changes+=[('head_changed',lambda x:x.update(logical_root_head_unchanged=99)),('no_absence_witness',lambda x:x.update(absence_physical_sequence=0)),('lost_reply_accepted',lambda x:x.update(lost_response_rejected=False)),('cancel_contacted',lambda x:x.update(canceled_before_get=False)),('stale_returned',lambda x:x['cases'][0]['returned_root'].update(Head=1)),('legacy_reset',lambda x:x['cases'][-1]['root'].update(Head=0))]
 if 'held' in d:changes+=[('held_forwarded',lambda x:x['held'].update(forwarded_bytes=1)),('truncated',lambda x:x['wire'].update(truncated=True)),('root_not_latest',lambda x:x['returned_root'].update(Head=1)),('not_rejected',lambda x:x.update(old_witness_rejected=False))]
 if d['scenario']=='read-witness-quorum-partition':changes+=[('minority_succeeded',lambda x:x.update(isolated_reads_rejected=False)),('heal_regressed',lambda x:x['healed_root'].update(Head=1)),('no_minority_error',lambda x:x.update(minority_before_error=''))]
 for name,change in changes:
  bad=copy.deepcopy(d);change(bad)
  try:check(bad,identity)
  except (AssertionError,KeyError,ValueError):controls.append(p.parent.name+'/'+name)
  else:raise AssertionError('accepted '+name)
 bad=copy.deepcopy(identity);bad['peers'][0]['embedding_commit']='foreign'
 try:check(d,bad)
 except AssertionError:controls.append(p.parent.name+'/foreign-commit')
 else:raise AssertionError('foreign accepted')
assert {(p['scenario'],p['replicas']) for p in proofs}=={('authority-read-witness',1),('authority-read-witness',3),('read-witness-replacement-race',1),('read-witness-replacement-race',3),('read-witness-quorum-partition',3)}
out.mkdir(parents=True,exist_ok=False)
for p in paths:
 target=out/p.parent.name;target.mkdir();shutil.copy2(p,target/p.name)
 if (p.parent/'object-fixture-identity.json').exists():shutil.copy2(p.parent/'object-fixture-identity.json',target/'object-fixture-identity.json')
(out/'review.json').write_text(json.dumps({'source':revision,'proofs':proofs,'mutations_rejected':controls,'scope':'Five native component cases: controlled stale/speculative GETs, real held conditional-witness CAS and real route minority failure/majority advance/heal. Does not prove OS/power loss, all arbitrary partitions, production adoption, concurrency/scale or full release.'},indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
print('READ_WITNESS_REVIEW',len(proofs),len(controls))
