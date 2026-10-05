from pathlib import Path
import json,hashlib,subprocess,re,datetime,zipfile,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-fanout-hosted-37333174296-20261005');r=root/'artifact';out=repo/'docs/scale/fanout-combined-boundaries-2026-10-05/hosted-37333174296';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
run=json.loads((root/'run.json').read_text());jobs=json.loads((root/'jobs.json').read_text());artifact=json.loads((root/'artifact.json').read_text());source='7a4d7392e0d2e28b624b7ea504e41223ec9a9f04'
assert run['id']==37333174296 and run['head_sha']==source and run['conclusion']=='success' and run['status']=='completed'
job=next(j for j in jobs['jobs'] if j['id']==111841179439);assert job['conclusion']=='success' and job['status']=='completed'
assert artifact['workflow_run']['id']==run['id'] and artifact['workflow_run']['head_sha']==source and artifact['id']==11355718693
zipsha=sha(root/'original-artifact.zip');assert artifact['digest']=='sha256:'+zipsha
index={}
with zipfile.ZipFile(root/'original-artifact.zip') as z:
 assert len(set(z.namelist()))==len(z.namelist())
 for m in z.infolist():
  if m.is_dir():continue
  p=Path(m.filename);assert not p.is_absolute() and '..' not in p.parts
  with z.open(m) as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
  assert sha(r/p)==digest and (r/p).stat().st_size==m.file_size
  index[m.filename]={'sha256':digest,'bytes':m.file_size}
(root/'artifact-file-index.json').write_text(json.dumps(index,indent=2)+'\n')
e=json.loads((r/'execution.json').read_text());b=json.loads((r/'binary.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and e['source']==source and e['full_six_boundary_selection'] and e['selected_case'] is None
assert sha(r/'integration.test')==e['sha256']==b['sha256']
assert e['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info'] and 'vcs.revision='+source in b['build_info']
assert subprocess.check_output(['go','version','-m',str(r/'integration.test')],text=True).splitlines()[1:]==b['build_info'].splitlines()[1:]
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==source
for name,digest in before['files'].items():assert sha(r/'source'/name)==digest==hashlib.sha256(subprocess.check_output(['git','show',source+':'+name],cwd=repo)).hexdigest()
external=json.loads((r/'external-source-before.json').read_text());assert external==json.loads((r/'external-source-after.json').read_text());paths=json.loads((r/'external-captured-paths.json').read_text());assert set(paths)==set(external)
for name,digest in external.items():assert sha(r/paths[name])==digest
producer=json.loads((r/'producer-source.json').read_text());assert producer['revision']==source
assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+producer['path']],cwd=repo)).hexdigest()==producer['sha256']==sha(r/'executed-producer.py')
joblog=(root/'native-job.log').read_text();assert f"NATIVE_STARTED {e['pid']} {source}" in joblog and 'NATIVE_FINISHED 0' in joblog
workflow=subprocess.check_output(['git','show',source+':.github/workflows/fanout-combined-boundaries.yml'],cwd=repo);(root/'executed-workflow.yml').write_bytes(workflow)
log=(r/'native.log').read_text();test='TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix';rows=[]
for phase in ('create','results'):
 for pos in ('first','interior','last'):
  name=phase+'/'+pos;leaf=r/'originals'/test/name
  terminal=re.findall(r'--- (PASS|FAIL): '+re.escape(test+'/'+name)+r' \(([0-9.]+)s\)',log);assert len(terminal)==1 and terminal[0][0]=='PASS'
  a=json.loads((leaf/'actual-parent-sdk.json').read_text());j=json.loads((leaf/'journal-restart.json').read_text());p=json.loads((leaf/'final-proof.json').read_text());prefix=a['durable_prefix']
  assert a['actual_sdk_sha256']==e['sha256'] and a['actual_sdk_build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
  cut=int((leaf/'parent-cut').read_text());assert cut=={'first':0,'interior':381 if phase=='create' else 363,'last':499}[pos]
  assert j['phase']==phase and j['library_restart_not_server_sigkill'] and j['old_server_id']!=j['new_server_id'] and 0<=j['node']<3
  start=datetime.datetime.fromisoformat(j['started'].replace('Z','+00:00'));heal=datetime.datetime.fromisoformat(j['healed'].replace('Z','+00:00'));observed=datetime.datetime.fromisoformat(a['observed_before_kill'].replace('Z','+00:00'));assert observed<start<heal
  for label in ('before','after'):
   info=j[label];assert info['config']['storage']=='file' and info['config']['num_replicas']==3 and len(info['cluster']['replicas'])==2 and all(peer['current'] and not peer.get('offline',False) for peer in info['cluster']['replicas'])
   assert info['state']['last_seq']>=prefix[-1]['sequence']
  assert j['captured_prefix_tail_sequence']==prefix[-1]['sequence'] and j['before']['state']['messages']==j['after']['state']['messages'] and j['before']['state']['last_seq']==j['after']['state']['last_seq']
  assert p['child_count']==500 and p['parent_result']=='249500' and p['creation_prefix' if phase=='create' else 'result_prefix']==prefix
  assert p['retained_integrity']['Invocations']==p['retained_integrity']['Journals']==p['retained_integrity']['Terminal']==501
  rows.append({'case':name,'seconds':float(terminal[0][1]),'cut':cut,'prefix_records':len(prefix),'prefix_tail_sequence':prefix[-1]['sequence'],'journal_tail_sequence':j['before']['state']['last_seq'],'actual_child_sdk_verified':True,'library_r3_restart_verified':True,'retained_integrity':p['retained_integrity']})
checker=subprocess.check_output(['git','show',source+':scripts/check-fanout-boundary-matrix.py'],cwd=repo);(root/'executed-acceptance-checker.py').write_bytes(checker)
with (root/'reconverted-events.jsonl').open('w') as f:subprocess.run(['go','tool','test2json','-p','js-wf/integration'],input=log,text=True,stdout=f,check=True)
subprocess.run(['python3',str(root/'executed-acceptance-checker.py'),'--combined','--events',str(root/'reconverted-events.jsonl'),'--output',str(root/'rechecked-acceptance.json')],check=True)
assert json.loads((root/'rechecked-acceptance.json').read_text())==json.loads((r/'acceptance.json').read_text())
# None of the original extracted artifact files were modified by review.
assert all(sha(r/name)==m['sha256'] for name,m in index.items()) and sha(root/'original-artifact.zip')==zipsha
review={'source':source,'run':run['id'],'job':job['id'],'artifact':artifact['id'],'original_artifact_sha256':zipsha,'actual_hosted_sdk_pid':e['pid'],'actual_hosted_sdk_sha256':e['sha256'],'source_files_verified':len(before['files']),'external_files_verified':len(external),'complete_zip_files_read_and_verified':len(index),'native_seconds':float(re.search(r'--- PASS: '+re.escape(test)+r' \(([0-9.]+)s\)',log).group(1)),'all_six_combined_boundaries_qualified_at_recorded_source':True,'cases':rows,'physical_drain_qualified':False,'independent_copied_integrity_pending':True,'full_release_qualified':False,'limits':['Hosted /proc observations bound through exact retained producer and native job log; root VM process namespace does not verify hosted process liveness.','Embedded R3 server libraries; not separate NATS process SIGKILL.','No original store reopen; final integrity/results/prefix claims retain native named-test scope.','Six boundary positions, not all500 positions or full fault matrix/24h.','Selected inputs exclude exhaustive assembly/embed/compiler inventory.','test2json conversion adds no execution timestamps.']}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py')
for name in ('run.json','jobs.json','artifact.json','native-job.log','executed-workflow.yml','artifact-file-index.json','executed-acceptance-checker.py','rechecked-acceptance.json','independent-review.json','executed-review.py'):shutil.copy2(root/name,out/name)
# Ship the exact authoritative uploaded ZIP once, not a duplicate tar of its expansion.
parts=[];h=hashlib.sha256()
with (root/'original-artifact.zip').open('rb') as f:
 i=0
 while data:=f.read(25*1024*1024):
  dest=out/f'original-artifact.zip.part-{i:03d}';dest.write_bytes(data);check=dest.read_bytes();assert check==data;h.update(check);parts.append({'file':dest.name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()});i+=1
assert h.hexdigest()==zipsha
(out/'archive-verification.json').write_text(json.dumps({'archive_format':'zip','archive_sha256':zipsha,'archive_bytes':(root/'original-artifact.zip').stat().st_size,'members':len(index),'parts':parts,'all_archive_file_members_and_parts_read_back':True,'github_artifact_digest_matches_exact_original_zip':True},indent=2)+'\n')
print(json.dumps(review),flush=True)
