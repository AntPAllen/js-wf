from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-chunked-r1-current-oracles-20261006');out=repo/'docs/scale/chunked-callback-audit-2026-10-06/current-oracles';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
qualified=e['status']=='passed' and e['exit_code']==0
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
assert json.loads((root/'commands.json').read_text())['env']['WF_AUDIT_CHUNKED_CALLBACK']=='1'
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestBatchedInvariantAuditNativeCompactionCohortAndFreshCorruption','TestBatchedInvariantAuditNativeJournalCorruption','TestBatchedInvariantAuditNativeStateValues'}
if qualified:assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
else:assert '--- FAIL:' in log
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
r1_observations=re.findall(r'full-oracle R1-direct stream=(\S+) source_replicas=(\d+) cursor=(\S+) cursor_replicas=(\d+) chunked=true',log)
if qualified:
 assert {x[0] for x in r1_observations}=={'WF_INV','WF_JRN'}
 assert all(x[1]=='3' and x[3]=='1' for x in r1_observations)
r={'r1_current_full_oracle_pass':qualified,'r1_cursor_observations':r1_observations,'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_race_test_passes_seconds':passed,'server_scope':'Current2.15.0 library servers share the observed SDK process/executable; actual SDK module identity and PID closure verified','source_capture_scope':'Selected Git Go/module inputs and actual executable metadata, not exhaustive external compiler/toolchain inputs','candidate_adopted':False,'qualifies_400k_capacity':False,'qualifies_24h':False}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
