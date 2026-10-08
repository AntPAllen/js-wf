import json,hashlib,subprocess,re,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-worker-2026-10-08/verification-after-reader-fix'
result=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert before['revision']==after['revision']==result['source'];assert before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for name,h in before['files'].items():
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==h,name
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',result['source']+':'+name],cwd=repo)).hexdigest()==h,name
assert [x['name'] for x in result['runs']]==['worker-normal','journal-normal','client-normal','worker-sim-normal','worker-race','journal-race','client-race','worker-sim-race']
checks=[]
for run in result['runs']:
 assert run['exit_code']==0 and len(run['package_passes'])==1
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(r.get('Action') in ('fail','skip') for r in rows)
 output=''.join(r.get('Output','') for r in rows)
 assert 'DATA RACE' not in output
 pins=[r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/')]
 graph=[p for p in pins if p.split('/')[1].startswith(('graph-publication-','graph-readers-','graph-reader-resume-','graph-catalog-','graph-application-','graph-journal-','graph-worker-'))]
 if run['name'].startswith('worker-sim'):
  assert len(pins)==len(set(pins))==503 and len(graph)==70
  seeds=1000 if run['name'].endswith('race') else 10000
  assert f'seeds_per_workload={seeds} generated_schedules={seeds}' in output
 if run['name'] in ('worker-normal','worker-race'):
  for replicas in (1,3):assert any(r.get('Action')=='pass' and r.get('Test')==f'TestNativeGraphWorkerReplayInputsSignalsAndResults/R{replicas}' for r in rows)
 if run['name'].startswith('journal-'):assert len(run['top_level_passes'])==8
 checks.append(dict(name=run['name'],package=run['package_passes'][0],top_groups=len(run['top_level_passes']),pinned_traces=len(pins),graph_pinned_traces=len(graph)))
report=dict(source=result['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,all_eight_commands_pass=True,checks=checks,scope='Focused graph worker/client/journal component verification only; complete 133-workload current-source qualification, canonical runtime migration, native crash/scale/partition/capacity/import/deployment and full original release gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
