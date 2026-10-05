from pathlib import Path
import json,hashlib,subprocess,shutil,tarfile
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-state-watch-mapped-client-copied-20261005');out=repo/'docs/scale/state-watch-leader-loss-2026-10-05/mapped-client-qualified';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and e['live_servers_captured'];assert sha(root/'integrity.test')==e['sha256'] and 'vcs.modified=false' in e['build_info'] and 'vcs.revision='+e['source'] in e['build_info'];assert not Path('/proc/'+str(e['pid'])).exists()
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
proof_path=next((root/'originals').rglob('state-watch-leader-loss.json'));proof=json.loads(proof_path.read_text());f=proof['fault'];assert f['triggered'] and not f['injection_error'];c=f['consumer'];assert c['num_pending']>0 and c['config']['num_replicas']==1 and c['config']['mem_storage'] and c['config']['ack_policy']=='none';assert c['cluster']['leader'].endswith('-n'+str(f['node']));assert f['kill']['node']==f['node'] and f['kill']['source_stopped'] and f['kill']['concurrent_observation'] and f['kill']['state']=='dead'
a,b=proof['attempts'];assert a['records']==11685 and not a['complete'] and 'initial_complete=false' in a['read_error'] and 'context deadline exceeded' in a['read_error'];assert not a['creation_error'];assert b['records']==29177 and b['complete'] and b['read_error']=='<nil>' and not b['creation_error'];assert proof['values']==29177 and proof['error']=='<nil>' and proof['elapsed_ns']==2241321269<20_000_000_000
client=json.loads((proof_path.parent/'client-after-snapshot.json').read_text());assert client['status']==1 and len(set(client['servers']))==5 and not client['discovered'] and client['connected_url'] in client['servers'];assert all(u.startswith('nats://127.0.0.1:') for u in client['servers'])
assert (proof_path.parent/f"server-{f['node']}-before-kill.log").stat().st_size>0
for i in range(5):
 if i!=f['node']:assert (proof_path.parent/f'server-{i}.log').exists()
log=(root/'native.log').read_text();assert 'PASS' in log and 'FAIL' not in log
review={'source':e['source'],'actual_sdk_sha256':e['sha256'],'five_actual_server_executables_and_builds_verified':True,'selected_git_inputs_verified':len(before['files']),'original_archive_and_store_files_independently_verified':len(copy['files']),'original_store_bytes_unchanged':True,'native_pass_seconds':14.49,'snapshot_elapsed_ns':proof['elapsed_ns'],'values':proof['values'],'attempts':proof['attempts'],'client':client,'fault':f,'qualifies_focused_copied_state_watch_leader_loss':True,'production_budgets_changed':False,'historical_24h_cause_confirmed':False,'discovery_as_failure_cause_confirmed':False,'qualifies_full_matrix_or_24h':False,'limitations':['Different killed leader from prior failed copied fixture; endpoint pool previously unobserved.','Real matrix already used the corrected client policy.','Copied-store snapshot only; no concurrent full runtime workload or journal audit.','Killed server log is pre-cut only; other four logs captured after snapshot.']}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py')
for name in ['independent-review.json','executed-review.py','execution.json']:shutil.copy2(root/name,out/name)
for name in ['state-watch-leader-loss.json','client-after-snapshot.json']:shutil.copy2(proof_path.parent/name,out/name)
shutil.copy2(root/'executed-producer.py',out/'executed-producer.py');shutil.copy2('/tmp/js-wf-preserve-read-proof-20261005.py',out/'executed-preserver.py');print(json.dumps(review))
