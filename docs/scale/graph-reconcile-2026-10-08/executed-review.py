import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-reconcile-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==583 and len(seeded)==138 and 'TestSeededGraphReconcileReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','65e6865','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==557
for n in oldpins:assert subprocess.check_output(['git','cat-file','blob','65e6865:'+n],cwd=repo)==(repo/n).read_bytes(),n
newpins=set(pins)-set(oldpins);assert len(newpins)==26 and all(Path(n).name.startswith('graph-reconcile-') for n in newpins)
assert [x['name'] for x in r['runs']]==['reconcile-normal','journal-normal','worker-normal','sim-normal','reconcile-race','journal-race','worker-race','sim-race']
expected={}
for pkg,pattern in [('reconcile','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal)')]:
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
 for tag in ('reconcile','journal'):
  if pkg==tag: assert tops==expected[tag],(run['name'],tops^expected[tag])
 if pkg=='reconcile':
  for replicas in (1,3): assert f'TestNativeGraphReconcileRepairsStartSignalAndTimerHistory/R{replicas}' in passed
  for kind in ('start','signal','timer','suspended'):assert f'TestGraphReconcileFailedReadCannotCertifyCursorPrefix/{kind}' in passed
  for mode in ('unknown','conflict','expired','stale','corrupt'):assert f'TestGraphReconcileLoopRetriesUncertaintyWithoutSkippingBoundary/{mode}' in passed
  assert 'TestGraphReconcileSignalCacheBindsInvocationGeneration' in passed
  assert 'TestGraphReconcileRejectsMissingConfiguration' in passed
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
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==583
  seeds=1000 if run['name'].endswith('race') else 100000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphReconcileReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph reconcile modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==26 and sum(map(int,modes.values()))==seeds
  assert len(tops)==3
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_eight_commands_pass=True,checks=checks,all_557_previous_pins_unchanged=True,current_seeded_workloads=138,current_pins=583,new_graph_reconcile_pins=26,scope='Focused graph reconciler migration. Native actual start/signal worker repair R1/R3; timer histories are prepared, modeled wakeups remain pending and graph drain follows explicit fixture cleanup. Other canonical lifecycle/purge/retention migration, full138 qualification, all original native/crash/scale/24h/million physical-drain/dependency/default-adoption/release gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
