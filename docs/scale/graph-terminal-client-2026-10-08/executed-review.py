import json,hashlib,subprocess,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-terminal-client-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==516 and len(seeded)==134 and 'TestSeededGraphResultReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','48f32b0','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==503
for n in oldpins:assert subprocess.check_output(['git','cat-file','blob','48f32b0:'+n],cwd=repo)==(repo/n).read_bytes(),n
assert all(Path(n).name.startswith('graph-result-') for n in set(pins)-set(oldpins))
assert [x['name'] for x in r['runs']]==['worker-normal','journal-normal','client-normal','result-sim-normal','worker-race','journal-race','client-race','result-sim-race']
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','skip') for x in rows)
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 passed=[x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test','').startswith('TestPinnedRegressionCorpus/')]
 if run['name'].startswith('result-sim'):
  assert set(passed)=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(passed)==516
  seeds=1000 if run['name'].endswith('race') else 10000
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert 'graph results: modes=' in output
 if run['name'].startswith('journal'):assert len(run['top_level_passes'])==12
 if run['name'].startswith('client'):
  assert len(run['top_level_passes'])==3
  for replicas in (1,3):
   for mode in ('inline','external','failed','cancelled','bad_generation','bad_kind'):
    assert any(x.get('Action')=='pass' and x.get('Test')==f'TestNativeGraphClientCanonicalTerminals/R{replicas}/{mode}' for x in rows)
 if run['name'].startswith('worker'):
  for replicas in (1,3):assert any(x.get('Action')=='pass' and x.get('Test')==f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' for x in rows)
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(run['top_level_passes']),pins=len(passed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_eight_commands_pass=True,checks=checks,all_503_previous_pins_unchanged=True,current_seeded_workloads=134,current_pins=516,new_graph_result_pins=13,scope='Focused canonical graph terminal-client verification; complete current-source simulation, other runtime readers/writers/purge/snapshot/import migration and all broad native/crash/scale/release gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
