import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-parent-worker-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==654 and len(seeded)==141 and 'TestSeededGraphParentWorkerReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','350a447','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==642
migration=json.loads((repo/'docs/scale/graph-parent-worker-2026-10-08-development/pin-migration.json').read_text())
changed={x['path']:x for x in migration['changed']};assert len(changed)==3
for n in oldpins:
 old=subprocess.check_output(['git','cat-file','blob','350a447:'+n],cwd=repo);new=(repo/n).read_bytes()
 if n not in changed:assert old==new,n
 else:
  proof=changed[n];assert proof['old_sha256']==hashlib.sha256(old).hexdigest() and proof['new_sha256']==hashlib.sha256(new).hexdigest()
  original,current=json.loads(old),json.loads(new)
  assert original['seed']==current['seed']==proof['seed'] and original['decisions']==current['decisions'] and original['workload']==current['workload']
newpins=set(pins)-set(oldpins);assert len(newpins)==12 and all(Path(n).name.startswith('graph-parent-worker-') for n in newpins)
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
  assert 'TestGraphClientBindingKeepsOriginalModeAndUsesProvidedTransports' in passed
 if pkg=='journal':assert 'TestGraphJournalExistingHistoryIsNotStaleAbsence' in passed
 if pkg=='worker':
  assert len(tops)==5
  for replicas in (1,3):
   assert f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' in passed
   for asyncMode in ('false','true'):
    for external in ('false','true'):
     assert f'TestNativeGraphChildResultTransferAndReplay/R{replicas}/async={asyncMode}/external-result={external}' in passed
  for replicas in (1,3):
   for mode in ('active','uninitialized','forged_mirror','purging','retired','retired_missing_inv','replaced','missing_unconfirmed','consumed_inline','consumed_external','consumed_corrupt'):
    assert f'TestNativeGraphWorkerParentNotifications/R{replicas}/{mode}' in passed
 pinsPassed={x for x in passed if x.startswith('TestPinnedRegressionCorpus/')}
 if pkg=='sim':
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==654
  seeds=1000 if run['name'].endswith('race') else 10000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={2*seeds}' in output
  for family,mode_label,count in [('ParentWorker','graph parent worker: modes',12),('TerminalWorker','graph terminal worker: modes',18)]:
   assert f'TIER1_SEEDS test=TestSeededGraph{family}Replay first=1 last={seeds} completed={seeds} requested={seeds}' in output
   coverage=re.search(re.escape(mode_label)+r'=map\[([^]]+)\]',output);assert coverage
   modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==count and sum(map(int,modes.values()))==seeds
  assert len(tops)==4
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_eight_commands_pass=True,checks=checks,all_639_other_previous_pins_unchanged=True,three_parent_notification_pin_migrations_preserve_seeds_decisions=True,current_seeded_workloads=141,current_pins=654,new_graph_parent_worker_pins=12,scope='Graph worker parent notification client binding. Native prepared R1/R3 held duplicates and actual child worker regression; model uses prepared parent/child histories and explicit retirement/expiry/drain. Source legacy invocation/signal/run/staging work remains retained. Read rechecks are not atomic publisher/purge fencing; full canonical publication/import/deployment, complete141 simulation and all original native/crash/scale/24h/million physical-drain/dependency/default-adoption/release requirements remain open.')
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());families=ci['jobs']['transport']['strategy']['matrix']['family'];assert len(families)==16 and 'ParentWorker' in families
for family in families:
 if family!='Corpus':assert 'TestSeededGraph'+family+'Replay' in seeded
report['ci_yaml_family_inventory_checked']=True
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
