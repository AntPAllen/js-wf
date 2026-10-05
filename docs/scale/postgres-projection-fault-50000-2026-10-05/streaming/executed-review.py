from pathlib import Path
import json,hashlib,subprocess,shutil,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-postgres-projection-fault-streaming-50000-20261005');out=repo/'docs/scale/postgres-projection-fault-50000-2026-10-05/streaming';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.load(open(r/'execution.json'));b=json.load(open(r/'binary.json'));assert e['status'] in ('passed','failed') and e['exit_code'] in (0,1) and not Path(f"/proc/{e['pid']}").exists();assert sha(r/'integration.test')==e['sha256']==b['sha256'] and e['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:] and 'vcs.modified=false' in b['build_info'] and '-race=true' not in b['build_info'] and 'vcs.revision='+e['source'] in b['build_info']
before=json.load(open(r/'source-before.json'));assert before==json.load(open(r/'source-after.json')) and before['revision']==e['source']
for n,d in before['files'].items():assert sha(r/'source'/n)==d==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()
external=json.load(open(r/'external-source-before.json'));assert external==json.load(open(r/'external-source-after.json'));paths=json.load(open(r/'external-captured-paths.json'));assert set(paths)==set(external)
for n,d in external.items():assert sha(r/paths[n])==d
p=next((r/'originals').rglob('projection-fault-proof.json'));proof=json.load(open(p));assert proof['count']==50000 and proof['postgres_fault'] and proof['native_test_failed']==(e['exit_code']!=0) and proof['all_results_checked_while_projection_stopped'] and proof['stopped_projection_lag']==100000 and proof['projection_process_reaped_sigkill'];assert proof['actual_projection_sdk_sha256']==e['sha256'] and proof['actual_projection_sdk_build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:] and not Path(f"/proc/{proof['actual_projection_sdk_pid']}").exists();assert proof['writer_backend_termination_confirmed'] and 0<proof['catchup_admitted_rows']<50000 and bool(proof['faulted_projection_error']) and proof['journal_leader_old_server_id']!=proof['journal_leader_new_server_id'] and proof['journal_all_three_replicas_current']
pg=json.load(open(r/'actual-postgres.json'));assert sha(r/'actual-postgres')==pg['actual_executable_sha256'];stopped=json.load(open(r/'postgres-stopped.json'));assert not stopped['State']['Running'] and stopped['State']['Pid']==0 and stopped['Id']==pg['container']['Id'];media=json.load(open(r/'postgres-media-copy-verification.json'));assert media['all_closed_sql_media_bytes_match']
for n,d in media['files'].items():assert sha(r/'postgres-stopped-data'/n)==d
log=(r/'native.log').read_text();assert 'started 50000 invocations' in log and 'completed and checked 50000 results' in log
assert proof['run_queue_drained_before_fault'] and proof['all_workflow_workers_joined_before_fault'] and proof['pinned_clients_refreshed_after_restart']
for name in ['WF_INV','WF_JRN','KV_WF_STATE','WF_PURGE']:assert proof[name+'_all_three_replicas_current']
def instant(key):return datetime.datetime.fromisoformat(proof[key].replace('Z','+00:00'))
assert instant('run_queue_drained_before_fault') <= instant('all_workflow_workers_joined_before_fault') < instant('fault_started') < instant('journal_leader_stopped') < instant('journal_leader_restarted') <= instant('pinned_clients_refreshed_after_restart')
for name in ['WF_INV','WF_JRN','KV_WF_STATE','WF_PURGE']:assert instant(name+'_all_three_replicas_current')>=instant('faulted_projection_stopped')

tracepath=next((r/'originals').rglob('projection-dependency-trace.json'));trace=json.load(open(tracepath));assert trace['counts']['WF_INV.OrderedConsumer']['started']>=2 and trace['counts']['WF_INV.Messages']['started']>=2 and trace['counts']['WF_INV.Next']['started']>0 and 'WF_INV.Fetch' not in trace['counts']
passed=e['exit_code']==0
if passed:
 assert 'PASS' in log and 'FAIL' not in log and 'worker exit:' not in log and proof['final_lag']==0
 pre=next((r/'originals').rglob('before-rebuild.jsonl'));post=next((r/'originals').rglob('after-rebuild.jsonl'));assert sha(pre)==sha(post)==proof['row_and_indexed_column_sha256']
 unique=set();count=0
 for line in pre.open():
  typ,id,status,attrs,data=json.loads(line);row=json.loads(data)
  assert typ==row['type']=='view-scale' and id==row['id']==f'job-{count:05d}' and status==row['status']=='completed' and json.loads(attrs)=={} and not row.get('attributes') and row['schema_version']==1 and row['last_index']==1 and row['inv_seq']>0 and row['journal_seq']>0 and row['started'] and datetime.datetime.fromisoformat(row['updated'].replace('Z','+00:00'))>=datetime.datetime.fromisoformat(row['started'].replace('Z','+00:00')) and row['inv_seq'] not in unique
  unique.add(row['inv_seq']);count+=1
 assert count==50000
else:assert 'FAIL' in log

errors=[call for call in trace['recent'] if call.get('error')]
review={'source':e['source'],'actual_parent_sdk_sha256':e['sha256'],'actual_killed_projection_sdk_sha256':proof['actual_projection_sdk_sha256'],'selected_git_inputs_verified':len(before['files']),'selected_external_inputs_verified':len(external),'actual_postgres_executable_sha256':pg['actual_executable_sha256'],'actual_postgres_version':pg['version'],'closed_postgres_media_files_verified':len(media['files']),'native_status':e['status'],'count':50000,'results_checked_before_fault':50000,'stopped_projection_lag':100000,'confirmed_partial_catchup_rows':proof['catchup_admitted_rows'],'writer_session_termination_and_library_journal_restart_admitted':True,'workflow_queue_drained_and_workers_joined_before_fault':True,'pinned_clients_refreshed':True,'all_four_projection_sources_current_before_replacement':True,'bounded_dependency_trace_error_calls':errors,'continuous_WF_INV_iterator_observed':True,'fatal_dependency_confirmed':False,'faulted_projection_error':proof['faulted_projection_error'],'native_seconds':float(__import__('re').search(r'--- (?:PASS|FAIL): TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocations \(([0-9.]+)s\)',log).group(1)),'full_lag_zero_and_rebuild_equality_qualified_in_this_case':passed,'qualifies_full_matrix_or_24h':False,'no_historical_failure_cause_or_fix_claim':True,'limitations':['Real R3 servers embedded in SDK, no separate NATS server process hashes or logs.','SQL server gracefully stopped after SDK; no PostgreSQL server SIGKILL.','SQL exposed rows and indexed columns compared; internal random generations intentionally excluded.','Selected input inventory excludes exhaustive race/assembly/embed/compiler provenance.','Native retained-state assertions have named-test scope; original NATS stores are closed and not independently reopened.','Shares VM with two live campaigns; no pressure attribution.']}
(r/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,r/'executed-review.py')
for n in ['execution.json','independent-review.json','executed-producer.py','executed-review.py','actual-postgres.json']:shutil.copy2(r/n,out/n)
shutil.copy2(p,out/p.name);shutil.copy2(tracepath,out/tracepath.name);shutil.copy2('/tmp/js-wf-preserve-read-proof-20261005.py',out/'executed-preserver.py');print(json.dumps(review))
