import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
step=next(x for x in workflow['jobs']['sdk-step-metadata']['steps'] if 'run' in x)
guard=step['run'].split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
for mode in ('race','normal'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit_code']==0 and receipt['end_ns']>receipt['start_ns']
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^--- (FAIL|SKIP):',log,re.M)
 # Execute the exact current workflow assertions against each retained log.
 guard=guard.replace("pathlib.Path('sdk-step-metadata.log').read_text()",'log')
 exec(guard,{'log':log})
 inputs=json.loads((out/(mode+'-source-inputs.json')).read_text())
 for name,sha in inputs.items():
  content=Path(name).read_bytes()
  if mode=='race' and name=='wf/select_many_metadata_test.go':
   content=(out/'race-select-many-metadata.go.txt').read_bytes()
   assert subprocess.check_output(['gofmt'],input=content)==Path(name).read_bytes()
  assert hashlib.sha256(content).hexdigest()==sha,name
 if mode=='race':
  executables=json.loads((out/'race-executables.json').read_text())
  assert set(executables)=={'wf','stepwire'}
  assert all(receipt['start_ns']<=x['captured_ns']<=receipt['end_ns'] for x in executables.values())
  assert all('-race=true' in x['build'] for x in executables.values())
 else:
  assert receipt['executables_captured']==[] # Short normal process binaries were missed; explicit limitation.
baseline=json.loads((out/'baseline-command.json').read_text())
assert baseline['actual_exit_code']==1
original=subprocess.check_output(['git','show',baseline['source_revision']+':wf/select_many.go'])
assert original==(out/'baseline-select-many.go.txt').read_bytes()
log=(out/'baseline.log').read_text()
for field in ('timer_step','fire_at','clock_domain','child_type','child_id'):
 assert f'--- FAIL: TestSelectManyRequestMetadataAdmission/null_case_{field} ' in log
assert log.count('index=0 data=42 used=true position=2 err=<nil>')==5
report={'accepted':True,'direct_context_controls':16,'positive_controls':2,'negative_controls':14,'baseline_null_fields_consuming_signal':5,'workflow_guard':'exact current SDK job assertions passed against both logs','source_inputs':{m:len(json.loads((out/(m+'-source-inputs.json')).read_text())) for m in ('normal','race')}}
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
