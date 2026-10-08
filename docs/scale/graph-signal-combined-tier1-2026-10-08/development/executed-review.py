import hashlib,json,pathlib,re,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-combined-tier1-2026-10-08/development'
results=[]
for name,group in [('combined-normal1000.jsonl','TestSeededGraphSignalRuntimeCombinedReplay'),('pins-normal.jsonl','TestPinnedRegressionCorpus'),('pins-race.jsonl','TestPinnedRegressionCorpus')]:
 data=(base/name).read_bytes();rows=[json.loads(s) for s in data.splitlines()]
 assert rows[-1]['Action']=='pass' and not rows[-1].get('Test'),name
 assert not any(r['Action']=='fail' or 'DATA RACE' in r.get('Output','') for r in rows),name
 passes={r['Test'] for r in rows if r['Action']=='pass' and r.get('Test')}
 assert {s for s in passes if '/' not in s}=={group}
 if group=='TestPinnedRegressionCorpus':
  want={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
  assert len(want)==731
  assert {s.split('/',1)[1] for s in passes if '/' in s}==want
 else:
  output=''.join(r.get('Output','') for r in rows)
  checkpoints=re.findall(r'TIER1_PROGRESS test=TestSeededGraphSignalRuntimeCombinedReplay completed=(\d+) requested=(\d+)',output)
  assert checkpoints==[(str(n),'1000') for n in range(100,1001,100)]
  assert '320 distinct combinations' in output
  maps=re.search(r'dimensions=\[(.*?)\]; all injected',output).group(1)
  counts=re.findall(r'map\[([^]]+)\]',maps)
  assert len(counts)==4
  for text,want in zip(counts,[7,4,3,4]):
   pairs=text.split();assert len(pairs)==want and sum(int(p.rsplit(':',1)[1]) for p in pairs)==1000
 results.append(dict(log=name,sha256=hashlib.sha256(data).hexdigest(),seconds=rows[-1]['Elapsed']))
pins=json.loads((base/'pins.json').read_text());assert len(pins)==3
for p in pins:
 data=(root/p['path']).read_bytes();trace=json.loads(data)
 assert hashlib.sha256(data).hexdigest()==p['sha256']
 assert trace['workload']==p['workload']=='graph_canonical_signal_runtime_combined'
 assert trace['seed']==p['seed'] and [d['chosen'] for d in trace['decisions'][:4]]==p['dimensions']
new={p['path'] for p in pins};old=0
for p in (root/'sim/testdata/regressions').glob('*.json'):
 rel=str(p.relative_to(root))
 if rel in new:continue
 assert p.read_bytes()==subprocess.check_output(['git','show','44fe6bf:'+rel]),rel
 old+=1
assert old==728
inventory=(base/'seeded-inventory.txt').read_text().splitlines()
assert len(inventory)==146 and 'TestSeededGraphSignalRuntimeCombinedReplay' in inventory
report=dict(scope='development controls; complete frozen current-suite qualification remains separate',results=results,seeds=1000,distinct_combinations=320,possible_combinations=336,new_pins=3,unchanged_pins=728,seeded_families=146,verdict='PASS')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
