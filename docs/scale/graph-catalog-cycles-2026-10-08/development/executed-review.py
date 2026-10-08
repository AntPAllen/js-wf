import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd();base=root/'docs/scale/graph-catalog-cycles-2026-10-08/development'
results=[]
checks={
 'controls-normal.jsonl':None,
 'controls-race.jsonl':None,
 'native-normal.jsonl':{'TestNativeGraphReconcileCanonicalPendingStarts','TestNativeGraphReconcileCanonicalSignals','TestNativeGraphReconcileTerminalProjection'},
 'native-race.jsonl':{'TestNativeGraphReconcileCanonicalPendingStarts','TestNativeGraphReconcileCanonicalSignals','TestNativeGraphReconcileTerminalProjection'},
 'pins-normal.jsonl':{'TestPinnedRegressionCorpus'},
 'pins-race-cpu-budget.jsonl':{'TestPinnedRegressionCorpus'},
 'families-normal1000.jsonl':{'TestSeededGraphStartRepairReplay','TestSeededGraphSignalRuntimeReplay','TestSeededGraphSignalRuntimeCombinedReplay','TestSeededGraphTerminalCatalogReplay'},
}
listing=subprocess.check_output(['go','test','./reconcile','-list','^TestGraphCanonical'],text=True)
control_names={s for s in listing.splitlines() if s.startswith('Test')}
for name,want in checks.items():
 if want is None:want=control_names
 data=(base/name).read_bytes();rows=[json.loads(s) for s in data.splitlines()]
 assert rows[-1]['Action']=='pass' and not rows[-1].get('Test'),name
 assert not any(r['Action']=='fail' or 'DATA RACE' in r.get('Output','') for r in rows),name
 passes={r['Test'] for r in rows if r['Action']=='pass' and r.get('Test')}
 assert {s for s in passes if '/' not in s}==want,(name,want)
 output=''.join(r.get('Output','') for r in rows)
 for group in want:
  if group.startswith('TestSeeded'):
   assert 'TIER1_SEEDS test='+group+' first=1 last=1000 completed=1000 requested=1000' in output
  if group.startswith('TestNative'):
   assert {group+'/R1',group+'/R3'}.issubset(passes)
 if 'TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation' in want:
  cases={s for s in passes if s.startswith('TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation/')}
  assert len(cases)==48
 if 'TestPinnedRegressionCorpus' in want:
  pins={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
  assert len(pins)==748 and {s.split('/',1)[1] for s in passes if '/' in s}==pins
 results.append(dict(log=name,sha256=hashlib.sha256(data).hexdigest(),seconds=rows[-1]['Elapsed']))
lineages=json.loads((base/'pin-lineage.json').read_text());assert len(lineages)==43
for v in lineages:
 old=subprocess.check_output(['git','show',v['prior_source']+':'+v['path']]);new=(root/v['path']).read_bytes()
 assert hashlib.sha256(old).hexdigest()==v['prior_sha256'] and hashlib.sha256(new).hexdigest()==v['current_sha256']
 a,b=json.loads(old),json.loads(new)
 assert {k:v for k,v in a.items() if k!='transport'}=={k:v for k,v in b.items() if k!='transport'}
 assert [e for e in b['transport'] if e['operation']!='graph_publication_catalog_high_water']==a['transport']
 additions=[e for e in b['transport'] if e['operation']=='graph_publication_catalog_high_water']
 assert len(additions)==v['added_watermarks'] and all(e['outcome']=='ok' for e in additions)
changed={v['path'] for v in lineages};unchanged=0
for p in (root/'sim/testdata/regressions').glob('*.json'):
 rel=str(p.relative_to(root))
 if rel in changed:continue
 assert p.read_bytes()==subprocess.check_output(['git','show','2e460a2:'+rel]),rel
 unchanged+=1
assert unchanged==705
negative=[json.loads(s) for s in (base/'unbounded-control.jsonl').read_bytes().splitlines()]
assert negative[-1]['Action']=='fail'
failed={r['Test'] for r in negative if r['Action']=='fail' and r.get('Test')}
for kind in ['start','signal','terminal']:
 assert any('/'+kind+'/' in s for s in failed)
prior=[json.loads(s) for s in (base/'pins-race.jsonl').read_bytes().splitlines()]
assert prior[-1]['Action']=='fail' and any('context deadline exceeded' in r.get('Output','') for r in prior)
report=dict(scope='Development controls; complete current/extended qualification remains separate',results=results,cycle_wrap_cases=48,migrated_pins=43,unchanged_pins=705,total_pins=748,unbounded_control='Expected FAIL for all three scanners',earlier_race='FAIL: ten-second per-schedule CPU watchdog',verdict='PASS for final selected controls')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
