from pathlib import Path
import json,hashlib,subprocess,shutil
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-projection-streaming-retained-50000-20261005');out=repo/'docs/scale/postgres-projection-fault-50000-2026-10-05/copied-retained-audit'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.load(open(r/'execution.json'));b=json.load(open(r/'binary.json'));assert e['status']=='passed' and e['exit_code']==0 and not Path(f"/proc/{e['pid']}").exists();assert e['actual_sha256']==b['sha256']==sha(r/'review-sdk') and e['actual_build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
inputs=json.load(open(r/'selected-inputs.json'));assert sha(r/'helper.go')==inputs['helper_git_sha256']==hashlib.sha256(subprocess.check_output(['git','show',inputs['helper_git_source']+':scripts/projection-retained-review.go.txt'],cwd=repo)).hexdigest()
for path,d in inputs['files'].items():
 assert sha(r/d['captured'])==d['sha256']
 p=Path(path)
 if p.is_relative_to(repo):assert d['sha256']==hashlib.sha256(subprocess.check_output(['git','show',inputs['production_source']+':'+str(p.relative_to(repo))],cwd=repo)).hexdigest()
for n,d in inputs['module_files'].items():assert sha(r/'selected-source'/n)==d==hashlib.sha256(subprocess.check_output(['git','show',inputs['production_source']+':'+n],cwd=repo)).hexdigest()
before=json.load(open(r/'copy-before.json'));after=json.load(open(r/'original-after.json'));assert before['all_initial_copy_bytes_match'] and after['unchanged'] and before['files']==after['original_closed_files']
original=Path(before['original_root'])
for n,d in before['files'].items():assert sha(original/n)==d
copy=json.load(open(r/'copy-after.json'))
for n,d in copy['closed_copy_files'].items():assert sha(r/'cluster'/n)==d
review=json.load(open(r/'retained-review.json'));assert review['report']=={'Invocations':50000,'Journals':50000,'Entries':100000,'Terminal':50000} and review['audit_ns']<20_000_000_000 and review['whole_review_ns']<20_000_000_000
assert len(review['queue_all_three_peers'])==3
for info in review['queue_all_three_peers']:assert info['state']['messages']==0 and info['state']['consumer_count']==review['drained_partition_consumers']
summary={'original_source':before['original_source'],'original_native_status':before['original_native_status'],'original_sdk_sha256':before['original_sdk_sha256'],'helper_git_source':inputs['helper_git_source'],'helper_git_sha256':inputs['helper_git_sha256'],'actual_review_sdk_sha256':e['actual_sha256'],'selected_inputs_verified':len(inputs['files']),'original_closed_files_verified_unchanged':len(before['files']),'report':review['report'],'audit_seconds':review['audit_ns']/1e9,'whole_review_seconds':review['whole_review_ns']/1e9,'drained_partition_consumers':review['drained_partition_consumers'],'all_three_peer_queues_physically_zero':True,'full_copied_retained_state_and_drain_qualified':True,'qualifies_original_projection_fault_recovery':False,'qualifies_historical_cause_or_full_matrix_or_24h':False,'limitations':['Only fresh copies reopened; no concurrent writers or injected faults during copied audit.','Library NATS servers embedded in actual captured review SDK, no separate process hashes/logs.','Selected inputs do not capture exhaustive compiler/assembly/embed provenance.','Does not replace original projection native verdict; original bytes unchanged.','Shares VM with two live campaigns; no pressure-cause claim.']}
out.mkdir(parents=True,exist_ok=True);(r/'independent-review.json').write_text(json.dumps(summary,indent=2)+'\n');shutil.copy2(__file__,r/'executed-review.py')
for n in ['execution.json','binary.json','selected-inputs.json','copy-before.json','original-after.json','retained-review.json','independent-review.json','executed-producer.py','executed-review.py','native.log']:shutil.copy2(r/n,out/n)
shutil.copy2('/tmp/js-wf-preserve-read-proof-20261005.py',out/'executed-preserver.py');print(json.dumps(summary))
