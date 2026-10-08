import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');parent=repo/'docs/scale/graph-start-repair-2026-10-08';base=parent/'verification-after-completion-retry'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==710 and len(seeded)==144 and 'TestSeededGraphStartReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
migration=json.loads((parent/'trace-migration.json').read_text())
original=migration['original_commit'];assert original=='13b80f2'
changes={x['path']:x for x in migration['changes']};assert len(changes)==15
worker_migration=json.loads((parent/'development-completion-retry/trace-migration.json').read_text());assert worker_migration['original_commit'].startswith('c194e8c') and len(worker_migration['changes'])==1
changes.update({x['path']:x for x in worker_migration['changes']})
assert len(changes)==16
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only',original,'--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==688
unchanged=0
for n in oldpins:
 old=subprocess.check_output(['git','cat-file','blob',original+':'+n],cwd=repo)
 current=(repo/n).read_bytes()
 if n in changes:
  item=changes[n];oldtrace=json.loads(old);newtrace=json.loads(current)
  assert hashlib.sha256(old).hexdigest()==item['old_sha256'] and hashlib.sha256(current).hexdigest()==item['new_sha256']
  assert oldtrace['seed']==newtrace['seed']==item['seed'] and oldtrace['decisions']==newtrace['decisions'] and item['choices_unchanged']
 else: assert current==old,n;unchanged+=1
assert unchanged==672
bound_migration=json.loads((parent/'development-contention-fix/trace-migration.json').read_text())
assert bound_migration['original_commit'].startswith('030b328') and len(bound_migration['changes'])==9 and len(bound_migration['unchanged'])==13
for item in bound_migration['changes']:
 name=item['path'];old=subprocess.check_output(['git','cat-file','blob',bound_migration['original_commit']+':'+name],cwd=repo);current=(repo/name).read_bytes()
 assert hashlib.sha256(old).hexdigest()==item['old_sha256'] and hashlib.sha256(current).hexdigest()==item['new_sha256']
 assert json.loads(old)['seed']==json.loads(current)['seed']==item['seed'] and json.loads(old)['decisions']==json.loads(current)['decisions'] and item['choices_unchanged']
for name in bound_migration['unchanged']:
 assert (repo/name).read_bytes()==subprocess.check_output(['git','cat-file','blob',bound_migration['original_commit']+':'+name],cwd=repo)
newpins=set(pins)-set(oldpins);assert len(newpins)==22 and all(Path(n).name.startswith('graph-start-repair-') for n in newpins)
assert [x['name'] for x in r['runs']]==[f'{p}-{m}' for m in ('normal','race') for p in ('ownership','client','reconcile','journal','worker','worker_model','start','sim')]
expected={}
for pkg,pattern in [('internal/graphpublication','.'),('client','.'),('reconcile','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal|TestGraphCanonicalStart)'),('worker','^(TestNativeGraphWorkerReplayInputsSignalsAndResults|TestNativeGraphChildResultTransferAndReplay|TestGraphChildReplayRejectsForgedProvenance|TestGraphSelectedChildRequiresExactRecordedSignal|TestNativeGraphWorkerParentNotifications|TestNativeCanonicalStartRecoveryWorkerReplayAndPurge)$')]:
 output=subprocess.check_output(['go','test','./'+pkg,'-list',pattern],cwd=repo,text=True)
 expected['ownership' if pkg=='internal/graphpublication' else pkg]=set(l for l in output.splitlines() if re.fullmatch(r'Test\w+',l))
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
 assert not skipped,skipped
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
 if pkg=='reconcile':
  assert len(tops)==33
  for replicas in (1,3):assert f'TestNativeGraphReconcileCanonicalPendingStarts/R{replicas}' in passed
  assert output.count('reopened graph-start loop recovered 3 durable cuts without caller input; effects=3')==2
  for test in ('TestGraphCanonicalStartScanDryRunUnknownAndPrefix','TestGraphCanonicalStartScanRetirementIsSupersededNotReenqueued','TestGraphCanonicalStartRepairDuringPreparedAppend'):assert test in passed
  for seed in range(1,33):
   for pinned in ('true','false'):assert f'TestGraphCanonicalStartRepairDuringPreparedAppend/seed{seed}/pinned{pinned}' in passed
 if pkg=='client':
  assert len(tops)==10
  for test in ('TestCanonicalStartRecoveryCannotReserveReplacementAfterRetirement','TestCanonicalStartAwaitPendingReplacementAndMissingReadyPointer','TestCanonicalStartAwaitSourceCommittedBeforeBinding','TestCanonicalStartAwaitMissingSourceCannotProvePurge','TestCanonicalBoundStartRepairRequiresExactSourceAndGeneration'):assert test in passed
 if pkg=='worker_model':
  assert len(tops)==2 and tops=={'TestSeededGraphWorkerReplay','TestGraphWorkerCompletionAppendReaderInterleaving'}
  for seed in range(1,33):
   for mode in ('reader_pin','unknown_completion'):assert f'TestGraphWorkerCompletionAppendReaderInterleaving/seed{seed}/{mode}' in passed
  seeds=1000 if run['name'].endswith('race') else 10000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'TIER1_SEEDS test=TestSeededGraphWorkerReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph worker: modes=map\[([^]]+)\]',output)
  assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==6 and sum(map(int,modes.values()))==seeds
 if pkg in ('sim','start'):
  if pkg=='sim':assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==710
  else:assert not pinsPassed
  seeds=1000 if run['name'].endswith('race') else 10000
  test='TestSeededGraphStartRepairReplay' if pkg=='sim' else 'TestSeededGraphStartReplay'
  label='graph start repair' if pkg=='sim' else 'graph start'
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test={test} first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(label+r': modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==(22 if pkg=='sim' else 18) and sum(map(int,modes.values()))==seeds
  assert len(tops)==(4 if pkg=='sim' else 1)
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
failed=json.loads((parent/'results.json').read_text());assert failed['source'].startswith('a646f02') and len(failed['runs'])==5 and failed['runs'][-1]['name']=='worker-normal' and failed['runs'][-1]['exit_code']==1
failed_after=json.loads((parent/'source-after.json').read_text());assert failed_after['unchanged']
second=json.loads((parent/'verification-after-lease-setup/results.json').read_text());assert second['source'].startswith('030b328') and len(second['runs'])==10 and second['runs'][-1]['name']=='reconcile-race' and second['runs'][-1]['exit_code']==1
assert json.loads((parent/'verification-after-lease-setup/source-after.json').read_text())['unchanged']
third=json.loads((parent/'verification-after-contention-fix/results.json').read_text());assert third['source'].startswith('c194e8c') and len(third['runs'])==10 and third['runs'][-1]['name']=='reconcile-race' and third['runs'][-1]['exit_code']==1
assert json.loads((parent/'verification-after-contention-fix/source-after.json').read_text())['unchanged']
report=dict(third_failed_frozen_campaign_preserved=True,worker_completion_retry_controls=64,migrated_worker_pins=1,second_failed_frozen_campaign_preserved=True,metadata_only_bound_repair_interleaving_controls=64,migrated_bound_scanner_pins=9,source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_sixteen_commands_pass=True,checks=checks,unchanged_old_pins=672,migrated_start_pins=15,migration_preserves_all_seeds_and_decisions=True,current_seeded_workloads=144,current_pins=710,new_start_repair_pins=22,original_failed_frozen_campaign_preserved=True,scope='Bounded authority-root discovery, quorum-confirmed lifecycle, token-bound pending recovery and metadata-only bound enqueue repair through existing fenced native loop, Await classification and retirement-race fixes. 32-seed staged-append interleaving includes pinned-recovery negative control. Reopened native R1/R3 loops execute 3 prepared durable cuts each with effect one; not process kills. Model scanner families use explicit fixture retirement/source corrections; no full144-family/original matrix/24h/million physical-drain/default adoption/release or production GC acceptance.')
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());families=ci['jobs']['transport']['strategy']['matrix']['family'];assert len(families)==19 and 'StartRepair' in families
for family in families:
 if family!='Corpus':assert 'TestSeededGraph'+family+'Replay' in seeded
assert 'TestGraphWorkerCompletionAppendReaderInterleaving' in (repo/'.github/workflows/graph-publication.yml').read_text()
assert '(TestGraphReconcile|TestNativeGraphReconcile|TestGraphCanonicalStart)' in (repo/'.github/workflows/graph-publication.yml').read_text()
report['ci_yaml_family_inventory_checked']=True
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
