import json,hashlib,subprocess,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-child-transfer-2026-10-08/verification-after-provenance'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==557 and len(seeded)==137 and 'TestSeededGraphChildReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','4b37bd8','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==553
for n in oldpins:assert subprocess.check_output(['git','cat-file','blob','4b37bd8:'+n],cwd=repo)==(repo/n).read_bytes(),n
newpins=set(pins)-set(oldpins);assert len(newpins)==4 and all(Path(n).name.startswith('graph-child-signal-') for n in newpins)
assert [x['name'] for x in r['runs']]==['worker-normal','wf-normal','client-normal','child-sim-normal','signal-sim-normal','worker-race','wf-race','client-race','child-sim-race','signal-sim-race']
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 assert '-timeout=5m' in run['command'] and '-count=1' in run['command']
 assert run['environment']['GOMAXPROCS']=='2' and run['environment']['GOMEMLIMIT']=='512MiB'
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','skip','build-fail') for x in rows)
 assert (base/(run['name']+'.stderr')).read_bytes()==b''
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 passed=[x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test','').startswith('TestPinnedRegressionCorpus/')]
 if run['name'].startswith('child-sim'):
  assert set(passed)=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(passed)==557
  seeds=1000 if run['name'].endswith('race') else 10000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert 'graph child transfer modes=' in output
  assert f'TIER1_SEEDS test=TestSeededGraphChildReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  assert len(run['top_level_passes'])==3
 if run['name'].startswith('wf'):assert len(run['top_level_passes'])==59
 if run['name'].startswith('signal-sim'):
  seeds=1000 if run['name'].endswith('race') else 10000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphChildSignalReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  assert 'graph child signal provenance modes=' in output
  assert len(run['top_level_passes'])==2
 if run['name'].startswith('client'):
  assert len(run['top_level_passes'])==3
  for replicas in (1,3):
   for mode in ('inline','external','failed','cancelled','bad_generation','bad_kind'):
    assert any(x.get('Action')=='pass' and x.get('Test')==f'TestNativeGraphClientCanonicalTerminals/R{replicas}/{mode}' for x in rows)
 if run['name'].startswith('worker'):
  assert len(run['top_level_passes'])==4
  for replicas in (1,3):
   assert any(x.get('Action')=='pass' and x.get('Test')==f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' for x in rows)
   for asyncMode in ('false','true'):
    for external in ('false','true'):
     expected=f'TestNativeGraphChildResultTransferAndReplay/R{replicas}/async={asyncMode}/external-result={external}'
     assert any(x.get('Action')=='pass' and x.get('Test')==expected for x in rows),expected
  for limited in ('false','true'):
   for mode in ('valid','missing','foreign_type','foreign_id','foreign_generation','foreign_reference','duplicate_request','ordinary_signal','ordinary_forged_declaration'):
    expected=f'TestGraphChildReplayRejectsForgedProvenance/limit={limited}/{mode}'
    assert any(x.get('Action')=='pass' and x.get('Test')==expected for x in rows),expected
 if run['name'].startswith('worker'):
  for external in ('false','true'):
   for mode in ('valid','missing','foreign_sequence','foreign_name','foreign_bytes','ordinary_signal'):
    expected=f'TestGraphSelectedChildRequiresExactRecordedSignal/external={external}/{mode}'
    assert any(x.get('Action')=='pass' and x.get('Test')==expected for x in rows),expected
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(run['top_level_passes']),pins=len(passed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_ten_commands_pass=True,checks=checks,all_553_previous_pins_unchanged=True,current_seeded_workloads=137,current_pins=557,new_graph_child_signal_pins=4,scope='Focused child-result ownership transfer verification. Modeled source terminals are prepared fixtures with pending child dispatch; actual native child workflows execute R1/R3 sync/async and both payload shapes. Graph drain requires explicit fixture lifecycle cleanup. Production child retention coordination, other canonical migration, full137 simulation and every original broad native/crash/scale/release gate remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
