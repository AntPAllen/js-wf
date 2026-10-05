from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-callback-audit-legacy-controls-20261005');out=repo/'docs/scale/callback-audit-delivery-2026-10-05/legacy-controls';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestStreamingAuditNativeLegacy211'}
assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
legacy=root/'originals/TestStreamingAuditNativeLegacy211/nats-server-2.11.17'
legacy_sha=sha(legacy)
legacy_info=subprocess.check_output(['go','version','-m',str(legacy)],text=True)
assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.11.17' in legacy_info
observed=re.findall(r'legacy-node node=(\d+) pid=(\d+) version=2.11.17 actual_sha256=([0-9a-f]+)',log)
assert {int(x[0]) for x in observed}=={0,1,2}
for node,pid,digest in observed:assert digest==legacy_sha and not Path('/proc/'+pid).exists()
r={'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_race_test_passes_seconds':passed,'legacy_actual_server_observations':observed,'legacy_server_sha256':legacy_sha,'legacy_build_info':legacy_info,'server_scope':'Three actual2.11.17 process executable hashes observed inside native test and matching retained legacy executable; all observed PIDs closed','source_capture_scope':'Selected Git Go/module inputs and actual executable metadata, not exhaustive external compiler/toolchain inputs','candidate_adopted':False,'qualifies_400k_capacity':False,'qualifies_24h':False}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
