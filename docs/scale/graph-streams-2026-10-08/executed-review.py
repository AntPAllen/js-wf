import json,hashlib,subprocess,shutil,re
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-streams-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
pins=(base/'source-pin-inventory.txt').read_text().splitlines();seeded=(base/'source-seeded-inventory.txt').read_text().splitlines()
assert len(pins)==670 and len(seeded)==142 and 'TestSeededGraphStreamsReplay' in seeded
assert subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'],cwd=repo,text=True).splitlines()==seeded
assert sorted(pins)==sorted(n for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
oldpins=subprocess.check_output(['git','ls-tree','-r','--name-only','2cd54bb','--','sim/testdata/regressions'],cwd=repo,text=True).splitlines();assert len(oldpins)==654
for n in oldpins: assert (repo/n).read_bytes()==subprocess.check_output(['git','cat-file','blob','2cd54bb:'+n],cwd=repo),n
newpins=set(pins)-set(oldpins);assert len(newpins)==16 and all(Path(n).name.startswith('graph-streams-') for n in newpins)
assert [x['name'] for x in r['runs']]==['ownership-normal','journal-normal','sim-normal','ownership-race','journal-race','sim-race']
expected={}
for pkg,pattern in [('internal/graphpublication','.'),('journal','^(TestGraphJournal|TestNativeGraphJournal)')]:
 output=subprocess.check_output(['go','test','./'+pkg,'-list',pattern],cwd=repo,text=True)
 expected['ownership' if pkg=='internal/graphpublication' else pkg]=set(l for l in output.splitlines() if re.fullmatch(r'Test\w+',l))
checks=[]
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 assert ('-race' in run['command'])==run['name'].endswith('-race')
 assert run['command'][:3]==['go','test','-p=1']
 assert run['environment']['SIM_COVERAGE_SUMMARY']=='1'
 assert ('-timeout=10m' if run['name']=='sim-normal' else '-timeout=5m') in run['command'] and '-count=1' in run['command']
 assert run['environment']['GOMAXPROCS']=='2' and run['environment']['GOMEMLIMIT']=='512MiB'
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action') in ('fail','skip','build-fail') for x in rows)
 assert (base/(run['name']+'.stderr')).read_bytes()==b''
 output=''.join(x.get('Output','') for x in rows);assert 'DATA RACE' not in output
 tops=set(run['top_level_passes']);pkg=run['name'].rsplit('-',1)[0]
 passed={x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test')}
 if pkg in ('ownership','journal'): assert tops==expected[pkg],(run['name'],tops^expected[pkg])
 if pkg=='ownership':
  assert sum(x.startswith('TestGraphStreams') for x in tops)==6
  for replicas in (1,3): assert f'TestNativeGraphStreamsAtomicLifecycleAndRetainedReaders/R{replicas}' in passed
 pinsPassed={x for x in passed if x.startswith('TestPinnedRegressionCorpus/')}
 if pkg=='sim':
  assert pinsPassed=={'TestPinnedRegressionCorpus/'+Path(n).name for n in pins} and len(pinsPassed)==670
  seeds=1000 if run['name'].endswith('race') else 100000
  assert run['environment']['SIM_SEEDS']==str(seeds)
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
  assert f'TIER1_SEEDS test=TestSeededGraphStreamsReplay first=1 last={seeds} completed={seeds} requested={seeds}' in output
  coverage=re.search(r'graph streams: modes=map\[([^]]+)\]',output);assert coverage
  modes=dict(re.findall(r'(\w+):(\d+)',coverage.group(1)));assert len(modes)==16 and sum(map(int,modes.values()))==seeds
  assert len(tops)==3
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(tops),pins=len(pinsPassed)))
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_six_commands_pass=True,checks=checks,all_654_previous_pins_unchanged=True,current_seeded_workloads=142,current_pins=670,new_graph_streams_pins=16,scope='Independent append forests under one original-head root CAS. Complete graphpublication package, existing graph journal compatibility controls, native R1/R3 adapter reopening/large bytes/drain, new seeded family100000 normal/1000 race and all670 pins. This is not canonical invocation/signal runtime adoption, persistent keyed idempotency lookup, process/crash/partition/capacity or full142/original native matrix/24h/million physical-drain/default dependency/adoption/release/production GC acceptance.')
import yaml
ci=yaml.safe_load((repo/'.github/workflows/graph-publication.yml').read_text());families=ci['jobs']['transport']['strategy']['matrix']['family'];assert len(families)==17 and 'Streams' in families
for family in families:
 if family!='Corpus':assert 'TestSeededGraph'+family+'Replay' in seeded
report['ci_yaml_family_inventory_checked']=True
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
