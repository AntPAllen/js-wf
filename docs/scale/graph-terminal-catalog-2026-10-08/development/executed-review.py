import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd();base=root/'docs/scale/graph-terminal-catalog-2026-10-08/development'
checks={
 'catalog-normal1000.jsonl':{'TestSeededGraphTerminalCatalogReplay'},
 'catalog-race1000.jsonl':{'TestSeededGraphTerminalCatalogReplay'},
 'scanner-normal-final.jsonl':{'TestGraphCanonicalTerminalScanBoundedDryRunUnknownAndRecovery','TestGraphCanonicalTerminalScanRetirementAndUncertainEnqueue'},
 'scanner-race-final.jsonl':{'TestGraphCanonicalTerminalScanBoundedDryRunUnknownAndRecovery','TestGraphCanonicalTerminalScanRetirementAndUncertainEnqueue'},
 'native-normal.jsonl':{'TestNativeGraphReconcileTerminalProjection'},
 'native-race.jsonl':{'TestNativeGraphReconcileTerminalProjection'},
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
 if 'TestSeededGraphTerminalCatalogReplay' in groups:
  assert 'TIER1_SEEDS test=TestSeededGraphTerminalCatalogReplay first=1 last=1000 completed=1000 requested=1000' in output
 if 'TestNativeGraphReconcileTerminalProjection' in groups:
  assert {'TestNativeGraphReconcileTerminalProjection/R1','TestNativeGraphReconcileTerminalProjection/R3'}.issubset(passes)
 if 'TestPinnedRegressionCorpus' in groups:
  want={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
  assert len(want)==748 and {s.split('/',1)[1] for s in passes if '/' in s}==want
 results.append(dict(log=name,sha256=hashlib.sha256(data).hexdigest(),seconds=rows[-1]['Elapsed']))
pins=json.loads((base/'pins.json').read_text());assert len(pins)==12
for p in pins:
 data=(root/p['path']).read_bytes();trace=json.loads(data)
 assert hashlib.sha256(data).hexdigest()==p['sha256']
 assert trace['workload']==p['workload']=='graph_terminal_catalog_recovery'
 assert trace['seed']==p['seed'] and trace['decisions'][0]['chosen']==p['mode']
new={p['path'] for p in pins};old=0
for p in (root/'sim/testdata/regressions').glob('*.json'):
 rel=str(p.relative_to(root))
 if rel in new:continue
 assert p.read_bytes()==subprocess.check_output(['git','show','3e7504e:'+rel]),rel
 old+=1
assert old==736
inventory=(base/'seeded-inventory.txt').read_text().splitlines()
assert len(inventory)==148 and 'TestSeededGraphTerminalCatalogReplay' in inventory
negative=[json.loads(s) for s in (base/'unbounded-control.jsonl').read_bytes().splitlines()]
assert negative[-1]['Action']=='fail' and any('self-generated read witnesses prevented catalog wrap' in r.get('Output','') for r in negative)
report=dict(scope='Component development controls; no complete current-suite claim',results=results,families=148,new_pins=12,unchanged_pins=736,pins=748,unbounded_control='Expected FAIL',verdict='PASS for final selected controls')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
