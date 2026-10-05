from pathlib import Path
import json,hashlib,subprocess,shutil,tarfile
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-failed-soak-copied-cohort-20261005');out=repo/'docs/scale/failed-soak-copied-cohort-2026-10-05';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and e['live_servers_captured'];assert sha(root/'integration.test')==e['sha256'] and 'vcs.modified=false' in e['build_info'] and 'vcs.revision='+e['source'] in e['build_info'];assert not Path('/proc/'+str(e['pid'])).exists()
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==e['source']
for n,d in before['files'].items():assert sha(root/'source'/n)==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for i,s in enumerate(servers):
 assert sha(root/f'actual-containers/server-{i}')==s['sha256']=='751a788956b200d9ed08fa7dff4587b772b98e1b3e82cef6ff15b69be052f42d'
 assert 'github.com/nats-io/nats-server/v2' in s['actual_proc_build_info'] and s['host_pid']==s['container']['State']['Pid']
 assert any(m['Source'].startswith(str(root/'copied-stores')) and m['Destination']=='/data' for m in s['container']['Mounts'])
copy=json.loads((root/'original-to-copy-verification.json').read_text());after=json.loads((root/'original-after-verification.json').read_text());assert copy['all_original_and_copy_bytes_match_verified_archive'] and after['all_original_store_bytes_still_match'] and len(copy['files'])==after['files']
original=Path(copy['original']);archive=original/'originals.tar.gz'
# Producer's archive path is confirmed below against the existing original archive.
if not archive.exists(): archive=original/'proof.tar.gz'
assert sha(archive)==copy['original_archive_sha256']
verified=set()
with tarfile.open(archive, 'r|gz') as t:
 for member in t:
  n=member.name
  if n not in copy['files']:continue
  d=copy['files'][n];f=t.extractfile(member);assert f is not None and hashlib.file_digest(f,'sha256').hexdigest()==d==sha(original/n);verified.add(n)
assert verified==set(copy['files'])
proof_path=next((root/'originals').rglob('copied-cohort-audit.json'));proof=json.loads(proof_path.read_text());assert proof['cutoff']==29120 and proof['error']=='<nil>' and proof['elapsed_ns']==5184059489<20_000_000_000
assert proof['report']=={'Invocations':29120,'Journals':29120,'Entries':321151,'Terminal':29120}
c=proof['trace']['counts']
for name in ['WF_INV','WF_JRN']:
 for op in ['CreateConsumer','Messages','DeleteConsumer']:
  v=c[name+'.'+op];assert v['started']==v['completed']==1 and v['errors']==0
assert c['WF_INV.Next']['started']==c['WF_INV.Next']['completed']==29120 and c['WF_INV.Next']['errors']==0
assert c['WF_JRN.Next']['started']==c['WF_JRN.Next']['completed']==321746 and c['WF_JRN.Next']['errors']==0 and c['WF_JRN.Next']['bytes']==48299508
for op in ['WatchAll','WatchStop']:
 v=c['WF_STATE.'+op];assert v['started']==v['completed']==1 and v['errors']==0
assert c['WF_STATE.KeyValue']['started']==c['WF_STATE.KeyValue']['completed']==2 and c['WF_STATE.KeyValue']['errors']==1
for i in range(5):assert (proof_path.parent/f'server-{i}.log').exists()
log=(root/'native.log').read_text();assert 'PASS' in log and 'FAIL' not in log
review={'source':e['source'],'actual_sdk_sha256':e['sha256'],'five_actual_server_executables_and_builds_verified':True,'selected_git_inputs_verified':len(before['files']),'original_archive_and_store_files_independently_verified':len(copy['files']),'original_store_bytes_unchanged':True,'native_pass_seconds':17.27,'copied_cohort_proof':proof,'original_20s_single_attempt_budget_unchanged':True,'qualifies_focused_complete_quiescent_copied_cohort_audit':True,'historical_24h_cause_confirmed':False,'qualifies_full_matrix_or_24h':False,'limitations':['No concurrent runtime writers or injected fault during this copied audit.','Shares VM with million-timer candidate; no causal pressure attribution.','Creation and cleanup tracing delegates original channels; no update consumption or initial-set event tracing.','Successful full checker establishes initial completion through its source contract, not a Stop success alone.','No speedup attribution against the original concurrent fault run.']}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py')
for name in ['independent-review.json','executed-review.py','execution.json']:shutil.copy2(root/name,out/name)
shutil.copy2(proof_path,out/proof_path.name);shutil.copy2(root/'executed-producer.py',out/'executed-producer.py');shutil.copy2('/tmp/js-wf-preserve-read-proof-20261005.py',out/'executed-preserver.py');print(json.dumps(review))
