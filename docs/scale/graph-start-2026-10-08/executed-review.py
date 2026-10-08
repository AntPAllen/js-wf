import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-start-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==688 and len(seeded)==143 and 'TestSeededGraphStartReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','e24aa11','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==670
for n in oldpins: assert (repo/n).read_bytes()==subprocess.check_output(['git','cat-file','blob','e24aa11:'+n],cwd=repo),n
newpins=set(pins)-set(oldpins);assert len(newpins)==18 and all(Path(n).name.startswith('graph-start-') for n in newpins)
assert [x['name'] for x in r['runs']]==[f'{p}-{m}' for m in ('normal','race') for p in ('client','journal','worker','retention','sim')]
expected={}
for pkg,pattern in [('client','.'),('retention','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal|TestGraphCanonicalStart)'),('worker','^(TestNativeGraphWorkerReplayInputsSignalsAndResults|TestNativeGraphChildResultTransferAndReplay|TestGraphChildReplayRejectsForgedProvenance|TestGraphSelectedChildRequiresExactRecordedSignal|TestNativeGraphWorkerParentNotifications|TestNativeCanonicalStartRecoveryWorkerReplayAndPurge)$')]:
 output=subprocess.check_output(['go','test','./'+pkg,'-list',pattern],cwd=repo,text=True)
 expected[pkg]=set(l for l in output.splitlines() if re.fullmatch(r'Test\w+',l))
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 assert ('-race' in run['command'])==run['name'].endswith('-race')
 assert run['command'][:3]==['go','test','-p=1']
 assert run['environment']['SIM_COVERAGE_SUMMARY']=='1'
 assert '-timeout=5m' in run['command'] and '-count=1' in run['command']
 assert run['environment']['GOMAXPROCS']=='2' and run['environment']['GOMEMLIMIT']=='512MiB'
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','build-fail') for x in rows)
 skipped={x.get('Test') for x in rows if x.get('Action')=='skip'}
 assert skipped==({'TestBlobSweepConcurrentRefreshContract'} if run['name'].startswith('retention-') else set())
 assert (base/(run['name']+'.stderr')).read_bytes()==b''
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 tops=set(run['top_level_passes']);pkg=run['name'].rsplit('-',1)[0]
 passed={x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test')}
 if pkg in expected: assert tops==expected[pkg]-skipped,(run['name'],tops^(expected[pkg]-skipped))
 if pkg=='journal': assert len(tops)==16
 if pkg=='worker':
  assert len(tops)==6
  for replicas in (1,3):
   for mode in ('ordinary','reserved','source_committed','bound_no_enqueue'):
    assert f'TestNativeCanonicalStartRecoveryWorkerReplayAndPurge/R{replicas}/{mode}' in passed
  assert output.count('physical drain: zero chunks, ')==2
 pinsPassed={x for x in passed if x.startswith('TestPinnedRegressionCorpus/')}
 if pkg=='sim':
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==688
  seeds=1000 if run['name'].endswith('race') else 10000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphStartReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph start: modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==18 and sum(map(int,modes.values()))==seeds
  assert len(tops)==4
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_ten_commands_pass=True,checks=checks,all_670_previous_pins_unchanged=True,current_seeded_workloads=143,current_pins=688,new_start_pins=18,retention_optional_boundary_not_executed=True,scope='Canonical graph Start staging, exact native source binding and explicit durable recovery, production worker retained-input execution and replay, production native purge/retained pins/high water. Prepared native restart cuts, not process kills; replacement terminals and model retirement manually prepared. Signals and discovery remain legacy; no deployed pending-start repair, complete143-family/original matrix/24h/million physical-drain/default adoption/release or production GC acceptance.')
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());families=ci['jobs']['transport']['strategy']['matrix']['family'];assert len(families)==18 and 'Start' in families
for family in families:
 if family!='Corpus':assert 'TestSeededGraph'+family+'Replay' in seeded
report['ci_yaml_family_inventory_checked']=True
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
