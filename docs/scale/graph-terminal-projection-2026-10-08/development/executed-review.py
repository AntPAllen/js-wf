import difflib,hashlib,json,pathlib,re,subprocess
root=pathlib.Path.cwd();base=root/'docs/scale/graph-terminal-projection-2026-10-08/development'
checks={
 'projection-normal1000.jsonl':{'TestSeededGraphTerminalProjectionReplay'},
 'existing-worker-normal1000-corrected.jsonl':{'TestSeededGraphTerminalWorkerReplay'},
 'families-race1000.jsonl':{'TestSeededGraphTerminalWorkerReplay','TestSeededGraphTerminalProjectionReplay'},
 'native-normal-receipt-baseline.jsonl':{'TestNativeGraphWorkerReplayInputsSignalsAndResults'},
 'native-race-receipt-baseline.jsonl':{'TestNativeGraphWorkerReplayInputsSignalsAndResults'},
 'pins-normal.jsonl':{'TestPinnedRegressionCorpus'},
 'pins-race.jsonl':{'TestPinnedRegressionCorpus'},
}
results=[]
for name,groups in checks.items():
 data=(base/name).read_bytes();rows=[json.loads(s) for s in data.splitlines()]
 assert rows[-1]['Action']=='pass' and not rows[-1].get('Test'),name
 assert not any(r['Action']=='fail' or 'DATA RACE' in r.get('Output','') for r in rows),name
 passes={r['Test'] for r in rows if r['Action']=='pass' and r.get('Test')}
 assert {s for s in passes if '/' not in s}==groups
 output=''.join(r.get('Output','') for r in rows)
 for group in groups:
  if group.startswith('TestSeeded'):
   assert 'TIER1_SEEDS test='+group+' first=1 last=1000 completed=1000 requested=1000' in output
  elif group=='TestPinnedRegressionCorpus':
   want={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
   assert len(want)==736 and {s.split('/',1)[1] for s in passes if '/' in s}==want
  else:assert {group+'/R1',group+'/R3'}.issubset(passes)
 results.append(dict(log=name,sha256=hashlib.sha256(data).hexdigest(),seconds=rows[-1]['Elapsed']))
pins=json.loads((base/'new-pins.json').read_text());assert len(pins)==5
assert {p['mode'] for p in pins}=={'missing','create_drop','create_lost_ack','concurrent_mirror','purge_at_create'}
for p in pins:
 data=(root/p['path']).read_bytes();trace=json.loads(data)
 assert hashlib.sha256(data).hexdigest()==p['sha256']
 assert trace['workload']==p['workload']=='graph_terminal_projection_repair'
 assert trace['seed']==p['seed'] and trace['decisions'][0]['chosen']==p['mode']
lineages=json.loads((base/'pin-lineage.json').read_text());assert len(lineages)==1
for v in lineages:
 old=subprocess.check_output(['git','show',v['prior_source']+':'+v['path']]);new=(root/v['path']).read_bytes()
 assert hashlib.sha256(old).hexdigest()==v['prior_sha256'] and hashlib.sha256(new).hexdigest()==v['current_sha256']
 a,b=json.loads(old),json.loads(new)
 assert {k:v for k,v in a.items() if k!='transport'}=={k:v for k,v in b.items() if k!='transport'}
 actual=[]
 aa=[json.dumps(e,sort_keys=True) for e in a['transport']];bb=[json.dumps(e,sort_keys=True) for e in b['transport']]
 for tag,x,y,u,w in difflib.SequenceMatcher(a=aa,b=bb,autojunk=False).get_opcodes():
  if tag!='equal':actual.append(dict(kind=tag,old=a['transport'][x:y],new=b['transport'][u:w]))
 assert actual==v['changes'] and len(actual)==4
 for change in actual:
  for event in change['old']+change['new']:
   assert event['operation'] in ('kv_create','kv_get','kv_delete') and event['subject']=='test.integrated-043'
excluded={p['path'] for p in pins}|{v['path'] for v in lineages};unchanged=0
for p in (root/'sim/testdata/regressions').glob('*.json'):
 rel=str(p.relative_to(root))
 if rel in excluded:continue
 assert p.read_bytes()==subprocess.check_output(['git','show','48196fb:'+rel]);unchanged+=1
assert unchanged==730
inventory=(base/'seeded-inventory.txt').read_text().splitlines()
assert len(inventory)==147 and 'TestSeededGraphTerminalProjectionReplay' in inventory
original=[json.loads(s) for s in (base/'original-control.jsonl').read_bytes().splitlines()]
assert original[-1]['Action']=='fail' and any('FAULT_SEED=6' in r.get('Output','') and 'terminal projection absent' in r.get('Output','') for r in original)
failed_native=[json.loads(s) for s in (base/'native-race.jsonl').read_bytes().splitlines()]
assert failed_native[-1]['Action']=='fail' and any('terminal ACK changed foreign lease' in r.get('Output','') for r in failed_native)
report=dict(scope='Development component controls; no complete current-source qualification claim',results=results,seeded_families=147,new_pins=5,migrated_pins=1,unchanged_pins=730,pins=736,original_control='Expected FAIL at seed 6',earlier_native_race='FAIL retained; cause unconfirmed',verdict='PASS for final selected controls')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
