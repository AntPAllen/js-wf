import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-purge-2026-10-08'
r=json.loads((base/'results.json').read_text());boundaryAfter=json.loads((base/'source-after-boundary.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
assert boundaryAfter['revision']==before['revision'] and boundaryAfter['files']==before['files'] and boundaryAfter['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==611 and len(seeded)==139 and 'TestSeededGraphPurgeReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','9098bbd','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==583
for n in oldpins:assert subprocess.check_output(['git','cat-file','blob','9098bbd:'+n],cwd=repo)==(repo/n).read_bytes(),n
newpins=set(pins)-set(oldpins);assert len(newpins)==28 and all(Path(n).name.startswith('graph-purge-') for n in newpins)
assert [x['name'] for x in r['runs']]==[label+'-'+mode for mode in ('normal','race') for label in ('retention','wf','reconcile','journal','worker','sim')]+['boundary-normal','boundary-race']
expected={}
for pkg,pattern in [('retention','.'),('wf','.'),('reconcile','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal)')]:
 output=subprocess.check_output(['go','test','./'+pkg,'-list',pattern],cwd=repo,text=True)
 expected[pkg]=set(l for l in output.splitlines() if re.fullmatch(r'Test\w+',l))
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 assert '-timeout=5m' in run['command'] and '-count=1' in run['command']
 assert run['environment']['GOMAXPROCS']=='2' and run['environment']['GOMEMLIMIT']=='512MiB'
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','build-fail') for x in rows)
 skips={x.get('Test') for x in rows if x.get('Action')=='skip'}
 assert skips==({'TestBlobSweepConcurrentRefreshContract'} if run['name'].startswith('retention-') else set())
 assert (base/(run['name']+'.stderr')).read_bytes()==b''
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 tops=set(run['top_level_passes']);pkg=run['name'].rsplit('-',1)[0]
 passed={x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test')}
 if pkg in expected:
  wanted=expected[pkg]-({'TestBlobSweepConcurrentRefreshContract'} if pkg=='retention' else set())
  assert tops==wanted,(run['name'],tops^wanted)
 if pkg=='boundary':
  assert tops=={'TestBlobSweepConcurrentRefreshContract'}
  assert run['environment']['WF_BLOB_BOUNDARY_ROOT'].startswith('/tmp/js-wf-graph-purge-boundary-')
  for shape in ('quiescent','refresh_after_census'):
   assert 'TestBlobSweepConcurrentRefreshContract/'+shape in passed
   proof=json.loads((base/(run['name']+'-'+shape+'-proof.json')).read_text())
   assert proof['dangling']==(shape=='refresh_after_census') and proof['old_nuid']!=proof['fresh_nuid'] and proof['acknowledged_reference']==proof['object']
 if pkg=='retention':
  for replicas in (1,3):
   assert f'TestNativeGraphRetentionWorkflowBindsAndPurgesTarget/R{replicas}' in passed
   for stage in ('fence','marker','signals','journal','timers','snapshot','native_timers','graph','tombstone','event','invocation'):
    assert f'TestNativeGraphPurgeEveryCommittedStageAndRetainedReader/R{replicas}/{stage}' in passed
 if pkg=='wf':
  for mode in ('valid','ordinary','before_request','duplicate_request','duplicate_signal','foreign_type','foreign_id','foreign_generation','foreign_ref','foreign_hash','forged_envelope','missing_result_edge','missing_envelope_edge','bad_inline_hash','failed','cancelled'):
   for external in ('false','true'):assert f'TestGraphChildRetentionRequiresExactParentOwnership/{mode}/external-envelope={external}' in passed
 if pkg=='reconcile':
  for replicas in (1,3):assert f'TestNativeGraphReconcileRepairsStartSignalAndTimerHistory/R{replicas}' in passed
  for kind in ('start','signal','timer','suspended'):assert f'TestGraphReconcileFailedReadCannotCertifyCursorPrefix/{kind}' in passed
  assert 'TestGraphReconcileSignalCacheBindsInvocationGeneration' in passed
 if pkg=='journal':assert 'TestGraphJournalPurgeFenceRetainsReadersAndGeneration' in passed
 if pkg=='worker':
  assert len(tops)==4
  for replicas in (1,3):
   assert f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' in passed
   for asyncMode in ('false','true'):
    for external in ('false','true'):assert f'TestNativeGraphChildResultTransferAndReplay/R{replicas}/async={asyncMode}/external-result={external}' in passed
 pinsPassed={x for x in passed if x.startswith('TestPinnedRegressionCorpus/')}
 if pkg=='sim':
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==611
  seeds=1000 if run['name'].endswith('race') else 100000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphPurgeReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph purge modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==28 and sum(map(int,modes.values()))==seeds
  assert len(tops)==3
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed),skips=sorted(skips)))
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());matrix=ci['jobs']['transport']['strategy']['matrix'];assert matrix['mode']==['normal','race'] and len(matrix['family'])==14
for family in matrix['family']:
 if family=='Corpus':continue
 assert 'TestSeededGraph'+family+'Replay' in seeded,family
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_twelve_commands_pass=True,all_fourteen_commands_pass=True,standard_retention_boundary_skip_covered_by_two_explicit_runs=True,checks=checks,all_583_previous_pins_unchanged=True,current_seeded_workloads=139,current_pins=611,new_graph_purge_pins=28,ci_yaml_and_family_inventory_checked=True,scope='Focused canonical graph retention. Native prepared committed-stage cuts and joined actual workers R1/R3, not process/storage crashes. Model uses prepared terminals, direct purge retries and explicit graph expiry/drain; negative fixtures and unrelated dispatch stay retained. Remaining canonical publication/state/snapshot/continuation/import/history/projection/CLI/deployment, complete139 qualification and all original native/scale/24h/million physical-drain/dependency/default-adoption/release gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
