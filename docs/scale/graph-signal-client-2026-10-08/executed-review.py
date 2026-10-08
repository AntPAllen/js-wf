import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-signal-client-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==642 and len(seeded)==140 and 'TestSeededGraphSignalClientReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','35b802b','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==611
for n in oldpins:assert subprocess.check_output(['git','cat-file','blob','35b802b:'+n],cwd=repo)==(repo/n).read_bytes(),n
newpins=set(pins)-set(oldpins);assert len(newpins)==31 and all(Path(n).name.startswith('graph-signal-client-') for n in newpins)
assert [x['name'] for x in r['runs']]==['client-normal','journal-normal','worker-normal','sim-normal','client-race','journal-race','worker-race','sim-race']
expected={}
for pkg,pattern in [('client','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal)')]:
 output=subprocess.check_output(['go','test','./'+pkg,'-list',pattern],cwd=repo,text=True)
 expected[pkg]=set(l for l in output.splitlines() if re.fullmatch(r'Test\w+',l))
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 assert '-timeout=5m' in run['command'] and '-count=1' in run['command']
 assert run['environment']['GOMAXPROCS']=='2' and run['environment']['GOMEMLIMIT']=='512MiB'
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','skip','build-fail') for x in rows)
 assert (base/(run['name']+'.stderr')).read_bytes()==b''
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 tops=set(run['top_level_passes']);pkg=run['name'].rsplit('-',1)[0]
 passed={x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test')}
 for tag in ('client','journal'):
  if pkg==tag: assert tops==expected[tag],(run['name'],tops^expected[tag])
 if pkg=='client':
  for replicas in (1,3):
   for mode in ('running','terminal','failed','cancelled','duplicate_inline','duplicate_external','duplicate_bad_hash','duplicate_missing_edge','fenced','generation_retired'):
    assert f'TestNativeGraphClientSignalsUseCanonicalHistory/R{replicas}/{mode}' in passed
 if pkg=='journal':assert 'TestGraphJournalExistingHistoryIsNotStaleAbsence' in passed
 if pkg=='worker':
  assert len(tops)==4
  for replicas in (1,3):
   assert f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' in passed
   for asyncMode in ('false','true'):
    for external in ('false','true'):
     assert f'TestNativeGraphChildResultTransferAndReplay/R{replicas}/async={asyncMode}/external-result={external}' in passed
 pinsPassed={x for x in passed if x.startswith('TestPinnedRegressionCorpus/')}
 if pkg=='sim':
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==642
  seeds=1000 if run['name'].endswith('race') else 100000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphSignalClientReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph signal client modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==31 and sum(map(int,modes.values()))==seeds
  assert len(tops)==3
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_eight_commands_pass=True,checks=checks,all_611_previous_pins_unchanged=True,current_seeded_workloads=140,current_pins=642,new_graph_signal_client_pins=31,scope='Canonical graph client signal decisions only. Native prepared R1/R3 histories and separate actual worker regression; model prepares consumption and retains legacy invocation/signal/run work. Read rechecks are not an atomic publication fence; incoming staging/publication and graph-aware worker parent publishers still need migration. Full140 qualification and all original native/crash/scale/24h/million physical-drain/dependency/default-adoption/release requirements remain open.')
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());families=ci['jobs']['transport']['strategy']['matrix']['family'];assert len(families)==15 and 'SignalClient' in families
for family in families:
 if family!='Corpus':assert 'TestSeededGraph'+family+'Replay' in seeded
report['ci_yaml_family_inventory_checked']=True
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
