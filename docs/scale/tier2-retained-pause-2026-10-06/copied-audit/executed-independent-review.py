from pathlib import Path
import json,hashlib,subprocess,sys
root=Path(sys.argv[1]);require_servers=sys.argv[2]=='v2'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
a=json.loads((root/'execution.json').read_text());assert a['status']=='passed' and a['exit_code']==0
assert not Path(f"/proc/{a['pid']}").exists()
assert sha(root/'review-sdk')==a['actual_sha256']
inputs=json.loads((root/'selected-inputs.json').read_text())
assert sha(root/'helper.go')==inputs['helper_git_sha256']
assert (root/'helper.go').read_bytes()==subprocess.check_output(['git','show',inputs['helper_git_source']+':scripts/matrix-retained-review.go.txt'])
for name,data in inputs['files'].items():assert sha(root/data['captured'])==data['sha256']
for name,h in inputs['module_files'].items():assert sha(root/'selected-source'/name)==h
before=json.loads((root/'copy-before.json').read_text());after=json.loads((root/'original-after.json').read_text());assert after['unchanged'] and before['files']==after['original_closed_files']
source=Path(before['original_root']);assert {str(p.relative_to(source)):sha(p) for p in source.rglob('*') if p.is_file()}==before['files']
review=json.loads((root/'retained-review.json').read_text())
expected=json.loads((root/'expected-original-report.json').read_text())
assert review['report']==expected
assert review['whole_review_ns']<20_000_000_000 and review['audit_ns']<20_000_000_000 and review['drained_partition_consumers']==64
for q in review['queue_all_three_peers']:assert q['state']['messages']==0 and q['state']['consumer_count']==64
servers=json.loads((root/'observed-servers.json').read_text())
if require_servers:
 assert {v['node'] for v in servers}=={0,1,2}
 assert (root/'executed-observer.py').read_bytes()==subprocess.check_output(['git','show',inputs['helper_git_source']+':scripts/matrix_process_observer.py'])
 for v in servers:
  assert not Path(f"/proc/{v['pid']}").exists()
  assert sha(root/v['captured'])==v['actual_executable_sha256']
else:assert not servers
report={'actual_sdk_pid':a['pid'],'actual_sdk_sha256':a['actual_sha256'],'original_production_source':inputs['production_source'],'helper_git_source':inputs['helper_git_source'],'selected_inputs':len(inputs['files']),'original_files_unchanged':len(before['files']),'retained_report':review['report'],'audit_ns':review['audit_ns'],'whole_review_ns':review['whole_review_ns'],'drained_three_clients_and_64_consumers':True,'server_observations':len(servers),'server_observation_required':require_servers,'original_smoke_only':False,'scope':'Independent copied integrity/history/drain review; original native fault qualification unchanged.'}
(root/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
