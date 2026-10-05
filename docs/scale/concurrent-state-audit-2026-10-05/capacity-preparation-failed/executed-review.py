from pathlib import Path
import json,subprocess,hashlib
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-concurrent-state-400k-capacity-20261005');out=repo/'docs/scale/concurrent-state-audit-2026-10-05/capacity-preparation-failed'
out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and e['exit_code']==1 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text())
for n,digest in a['files'].items():
 assert sha(root/'source'/n)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==digest
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for x in servers:
 assert not Path('/proc/'+str(x['host_pid'])).exists()
 assert sha(root/'actual-containers'/x['sha256'])==x['sha256']
log=(root/'native.log').read_text();assert '--- FAIL: TestConcurrentStateR5PopulationCapacity (35.96s)' in log and 'context deadline exceeded' in log
assert not list(root.rglob('capacity-comparison.json'))
r={'execution':e,'selected_git_source_inputs_verified':len(a['files']),'all_observed_processes_closed':True,'sdk_sha256':e['sha256'],'server_observations':len(servers),'no_population_or_audit_result':True,'stage':'Provisioning preparation deadline exceeded; cause not attributed','original_failure_preserved':True,'unchanged_native_rerun':False}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('FAILED_PREPARATION_PRESERVED',flush=True)
