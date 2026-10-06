from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-latency-acquisition-native-20261006');out=repo/'docs/scale/bulk-final-latency-preparation-2026-10-06/native-bulk-oracle';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
qualified=e['status']=='passed' and e['exit_code']==0
b=json.loads((root/'binary.json').read_text());assert sha(root/'integration.test')==b['sha256']==e['sha256'];assert '-race=true' in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
assert json.loads((root/'commands.json').read_text())['env']['WF_MATRIX_PARALLEL_LATENCY_ROOT']==str(root/'fixture')
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestMatrixParallelInvocationAuditsNativeOracle'}
if qualified:assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
else:assert '--- FAIL:' in log
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
oracle=json.loads((root/'fixture/oracle.json').read_text()) if qualified else None
if qualified:
 assert oracle['bulk_equals_serial'] and oracle['bulk_stats']['snapshot_fallbacks']==0 and oracle['bulk_stats']['charged_bytes']<3*1024**3 and oracle['legacy_equals_shared_reducer'] and oracle['invocations']==160 and oracle['serial_equals_parallel'] and oracle['deadline_negative_control'] and oracle['cached_metadata_equals_serial']
 assert oracle['metadata_handle_lookups']=={'journal':1,'signals':1,'state':1,'objects':0}
 assert len(oracle['samples'])==160 and {s['type'] for row in oracle['samples'] for s in row}=={'latencyshort','latencytimer','latencysignal','latencyparent','latencychild'}
r={'native_oracle_pass':qualified,'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_race_test_passes_seconds':passed,'server_scope':'Current2.15 library servers inside observed SDK; actual executable module identity and PID closure verified','oracle':oracle,'qualifies_scale':False,'qualifies_faults':False,'qualifies_24h':False}

shutil.copy2(repo/'integration/matrix_latency_legacy_oracle_test.go',root/'frozen-legacy-oracle.go.txt')
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
