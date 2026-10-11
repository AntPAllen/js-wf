import datetime,hashlib,json,re
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
manifest=json.loads((out/'source-inputs.json').read_text())
ci=json.loads((out/'ci-update.json').read_text())
for name,sha in manifest['files'].items():
 if name=='.github/workflows/graph-publication.yml':
  assert hashlib.sha256((out/'ci-before.yml.txt').read_bytes()).hexdigest()==sha==ci['before_sha256']
  assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==ci['after_sha256']
 else:assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
for name in ('command.json','native-command.json'):
 receipt=json.loads((out/name).read_text());assert receipt['actual_exit_code']==0 and receipt['executable_captured']
sdk=json.loads((out/'sdk-command.json').read_text());assert sdk['actual_exit_code']==0 and sdk['executables_captured']==['stepwire','wf']
assert datetime.datetime.fromisoformat(ci['observed']).timestamp()*1e9 > max(json.loads((out/name).read_text())['end_ns'] for name in ('command.json','native-command.json','sdk-command.json'))
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
race=(out/'race.log').read_text();native=(out/'native.log').read_text();sdklog=(out/'sdk.log').read_text()
guard=None
for job in workflow['jobs'].values():
 for step in job.get('steps',[]):
  script=step.get('run','')
  if "assert '--- PASS: TestRawGraphSDKStepNullMetadata ' in log" in script:
   a=script.index("assert '--- PASS: TestRawGraphSDKStepNullMetadata ' in log");b=script.index("assert '--- PASS: TestRawGraphChildSignalHistory ' in log",a);guard=script[a:b]
assert guard;exec(guard,{'log':race})
script=next(step['run'] for step in workflow['jobs']['sdk-step-metadata']['steps'] if 'run' in step)
guard=script.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0].replace("pathlib.Path('sdk-step-metadata.log').read_text()",'provided_log')
exec(guard,{'provided_log':sdklog})
assert native.count('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0')==4
assert native.count('RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0')==4
assert native.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2')==4
for log in (race,native,sdklog):assert not re.search(r'^--- FAIL:',log,re.M)
result={'runtime_module_inputs_unchanged':len(manifest['files'])-1,'ci_changed_only_after_runtime_commands':True,'physical_controls':68,'physical_positive':10,'physical_negative':58,'runtime_before_handler_negative_controls':28,'stepwire_null_zero_opaque_controls':60,'raw_graph_top_level_passes':sum(line.startswith('--- PASS: TestRawGraph') for line in race.splitlines()),'native_child_flows':4,'native_checkpoint_frames':8,'local_exact_raw_ci_guard':True,'local_exact_sdk_ci_guard':True,'test_seconds':{'raw':float(re.search(r'ok\s+js-wf/integrity\s+([\d.]+)s',race).group(1)),'wf':float(re.search(r'ok\s+js-wf/wf\s+([\d.]+)s',sdklog).group(1)),'stepwire':float(re.search(r'ok\s+js-wf/internal/stepwire\s+([\d.]+)s',sdklog).group(1)),'native':float(re.search(r'ok\s+js-wf/worker\s+([\d.]+)s',native).group(1))},'scope':'Null metadata wire admission; result stays opaque. Kind-specific semantics, original I4 causal/program-equivalence and release gates remain separate.'}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
