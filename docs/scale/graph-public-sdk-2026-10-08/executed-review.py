import json,hashlib,subprocess,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-public-sdk-2026-10-08'
r=json.loads((base/'results.json').read_text());before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert r['source']==before['revision']==after['revision'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for n,h in before['files'].items():
 assert hashlib.sha256((repo/n).read_bytes()).hexdigest()==h,n
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',r['source']+':'+n],cwd=repo)).hexdigest()==h,n
assert [x['name'] for x in r['runs']]==['journal-normal','worker-normal','journal-race','worker-race','external-consumer']
for run in r['runs']:
 assert run['exit_code']==0 and len(run['package_results'])==1
 rows=[json.loads(l) for l in (base/(run['name']+'.jsonl')).read_text().splitlines()]
 assert not any(x.get('Action')=='fail' for x in rows)
 assert 'DATA RACE' not in ''.join(x.get('Output','') for x in rows)
 if run['name']=='external-consumer':
  assert run['package_results'][0]['package']=='graph-sdk-consumer' and not run['top_level_passes']
  assert run['package_results'][0]['action']=='skip' # Compiled package has no tests; never claim runtime execution.
 else:
  assert run['package_results'][0]['action']=='pass' and not any(x.get('Action')=='skip' for x in rows)
  pattern='TestNativeGraphJournalPublicSDK' if run['name'].startswith('journal') else 'TestNativeGraphWorkerReplayInputsSignalsAndResults'
  for replicas in (1,3):assert any(x.get('Action')=='pass' and x.get('Test')==f'{pattern}/R{replicas}' for x in rows)
  if run['name'].startswith('journal'):assert len(run['top_level_passes'])==11
consumer=base/'external-consumer'
assert 'module graph-sdk-consumer\n' in (consumer/'go.mod').read_text()
assert 'js-wf/internal' not in (consumer/'consumer.go').read_text()
report=dict(source=r['source'],tracked_inputs=len(before['files']),committed_inputs_unchanged=True,four_native_normal_race_commands_pass=True,independent_module_compiles_public_types=True,consumer_runtime_executed=False,consumer_files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in consumer.iterdir() if p.is_file()},scope='Focused native public SDK admission, journal and worker verification. Existing simulation families/pins unchanged but full current-source qualification, canonical runtime migration and all broad release gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(report))
