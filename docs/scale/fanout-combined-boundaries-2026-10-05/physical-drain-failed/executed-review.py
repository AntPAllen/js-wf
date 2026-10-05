from pathlib import Path
import json,hashlib,subprocess,shutil,re,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-fanout-combined-physical-drain-20261005');out=repo/'docs/scale/fanout-combined-boundaries-2026-10-05/physical-drain-failed';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.load(open(r/'execution.json'));b=json.load(open(r/'binary.json'));assert e['status']=='failed' and e['exit_code']==1 and not Path(f"/proc/{e['pid']}").exists();assert sha(r/'integration.test')==e['sha256']==b['sha256'];assert e['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info'] and 'vcs.revision='+e['source'] in b['build_info']
before=json.load(open(r/'source-before.json'));assert before==json.load(open(r/'source-after.json')) and before['revision']==e['source']
assert e['full_six_boundary_selection'] and e['selected_case'] is None
producer=json.load(open(r/'producer-source.json'));assert producer['revision']==e['source'];assert hashlib.sha256(subprocess.check_output(['git','show',producer['revision']+':'+producer['path']],cwd=repo)).hexdigest()==producer['sha256']==sha(r/'executed-producer.py')
for n,d in before['files'].items():assert sha(r/'source'/n)==d==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()
external=json.load(open(r/'external-source-before.json'));assert external==json.load(open(r/'external-source-after.json'));paths=json.load(open(r/'external-captured-paths.json'));assert set(paths)==set(external)
for n,d in external.items():assert sha(r/paths[n])==d
log=(r/'native.log').read_text();test='TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix';cases=[]
for phase in ['create','results']:
 for position in ['first','interior','last']:
  case=phase+'/'+position;leaf=r/'originals'/test/case;term=re.findall(r'--- (PASS|FAIL): '+re.escape(test+'/'+case)+r' \(([0-9.]+)s\)',log);assert len(term)==1;status,seconds=term[0];assert status==('PASS' if case=='results/interior' else 'FAIL')
  row={'case':case,'native_status':status,'seconds':float(seconds),'actual_parent_sigkill_and_library_restart_admitted':False}
  if (leaf/'actual-parent-sdk.json').exists():
   a=json.load(open(leaf/'actual-parent-sdk.json'));j=json.load(open(leaf/'journal-restart.json'));prefix=a['durable_prefix'];assert a['actual_sdk_sha256']==e['sha256'] and a['actual_sdk_build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:] and not Path(f"/proc/{a['pid']}").exists();assert (leaf/'parent-cut').read_text()==str({'first':0,'last':499}.get(position,381 if phase=='create' else 363));assert j['captured_prefix_tail_sequence']==prefix[-1]['sequence'];assert j['phase']==phase and j['library_restart_not_server_sigkill'] and j['old_server_id']!=j['new_server_id'] and 0<=j['node']<3
   start=datetime.datetime.fromisoformat(j['started'].replace('Z','+00:00'));heal=datetime.datetime.fromisoformat(j['healed'].replace('Z','+00:00'));observed=datetime.datetime.fromisoformat(a['observed_before_kill'].replace('Z','+00:00'));assert observed<start<heal
   for label in ['before','after']:
    info=j[label];assert info['config']['storage']=='file' and info['config']['num_replicas']==3 and len(info['cluster']['replicas'])==2 and all(p['current'] and not p.get('offline',False) for p in info['cluster']['replicas']);assert info['state']['last_seq']>=prefix[-1]['sequence']
   assert j['before']['state']['messages']==j['after']['state']['messages'] and j['before']['state']['last_seq']==j['after']['state']['last_seq']
   row.update(actual_parent_sigkill_and_library_restart_admitted=True,prefix_records=len(prefix),prefix_tail_sequence=prefix[-1]['sequence'],journal_live_messages=j['before']['state']['messages'],journal_tail_sequence=j['before']['state']['last_seq'])
  if status=='PASS':
   p=json.load(open(leaf/'final-proof.json'));assert p['creation_prefix' if phase=='create' else 'result_prefix']==prefix;assert p['child_count']==500 and p['parent_result']=='249500';assert p['retained_integrity']['Invocations']==p['retained_integrity']['Journals']==p['retained_integrity']['Terminal']==501
  if status=='PASS':
   drain=json.load(open(leaf/'physical-drain.json'));assert drain['workers_joined'] and 0<drain['elapsed_ns']<30_000_000_000 and len(drain['all_three_peers'])==3
   for peer in drain['all_three_peers']:
    assert peer['queue']['state']['messages']==0 and peer['queue']['state']['consumer_count']==64 and len(peer['consumers'])==64
    assert {c['name'] for c in peer['consumers']}=={f'WF_P_{part:02d}' for part in range(64)} and all(c['num_pending']==c['num_ack_pending']==0 for c in peer['consumers'])
   row['physical_drain_verified_in_named_test']=True
  else:
   failure=json.load(open(leaf/'physical-drain-failure.json'));assert failure['deadline_error']=='context deadline exceeded'
   row['physical_drain_failure']=failure
  cases.append(row)
assert sum(c['actual_parent_sigkill_and_library_restart_admitted'] for c in cases)==6
helper_source=e['source'];helper=repo/'scripts/check-fanout-boundary-matrix.py';data=subprocess.check_output(['git','show',helper_source+':scripts/check-fanout-boundary-matrix.py'],cwd=repo);assert helper.read_bytes()==data;(r/'executed-acceptance-checker.py').write_bytes(data)
with (r/'converted-events.jsonl').open('w') as f:subprocess.run(['go','tool','test2json','-p','js-wf/integration'],input=log,text=True,stdout=f,check=True)
with (r/'acceptance-rejection.log').open('w') as f:guard=subprocess.run(['python3',str(r/'executed-acceptance-checker.py'),'--combined','--physical-drain','--events',str(r/'converted-events.jsonl'),'--output',str(r/'acceptance.json')],stdout=f,stderr=subprocess.STDOUT)
assert guard.returncode!=0 and not (r/'acceptance.json').exists()
review={'source':e['source'],'actual_parent_sdk_sha256':e['sha256'],'selected_git_inputs_verified':len(before['files']),'selected_external_inputs_verified':len(external),'native_status':'failed','native_seconds':float(re.search(r'--- FAIL: '+re.escape(test)+r' \(([0-9.]+)s\)',log).group(1)),'cases':cases,'full_combined_matrix_qualified':False,'physical_drain_matrix_qualified':False,'one_named_physical_drain_case_passed':True,'acceptance_checker_requires_all_six_passes':True,'qualifies_full_release_matrix_or_24h':False,'checker_git_source':helper_source,'checker_sha256':sha(r/'executed-acceptance-checker.py'),'limitations':['Native R3 servers embedded in race SDK; no separate server process SIGKILL.','Original stores closed; no independent reopen yet.','Only results/interior reached final integrity/prefix/child-result and all-peer physical drain assertions; named-test scope. Five failed new diagnostic30s drain bounds; original5m/30scut intact.','Conversion timestamps are not original execution timestamps.','Sequence coverage checked across snapshot purging; named-test final prefix equality/higher-epoch assertions.','Prior failed parent remains failed; no historical server-fault cause claim.','Selected source inventory excludes exhaustive assembly/embed/compiler inputs.']}
(r/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,r/'executed-review.py')
for n in ['execution.json','independent-review.json','executed-producer.py','executed-review.py','acceptance-rejection.log']:shutil.copy2(r/n,out/n)
print(json.dumps(review))
