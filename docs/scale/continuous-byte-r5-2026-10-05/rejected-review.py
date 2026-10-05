from pathlib import Path
import json,hashlib,subprocess,tarfile
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/continuous-byte-r5-2026-10-05';reports=[]
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for label,name,test in [('interrupted-cohort','js-wf-r5-continuous-interrupted-20261005','TestStreamingAuditR5RetainedProfileDiagnostic'),('traced-cohort','js-wf-r5-traced-cohort-20261005','TestRetainedAuditTraceR5FullCohort')]:
 root=Path('/tmp')/name;execution=json.loads((root/'execution.json').read_text());assert execution['status']=='passed' and execution['exit_code']==0 and execution['actual_containers_captured']
 before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
 for n,d in before['files'].items():assert sha(root/'source'/n)==d==hashlib.sha256(subprocess.check_output(['git','show',before['revision']+':'+n],cwd=repo)).hexdigest()
 binary=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==binary['sha256']==execution['exe_sha256'];assert f"vcs.revision={before['revision']}" in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
 hashes=json.loads((root/'actual-containers/binary-hashes.json').read_text());assert len(hashes)==5 and len(set(hashes.values()))==1
 for n,d in hashes.items():assert sha(root/'actual-containers'/n)==d
 inspect=json.loads((root/'actual-containers/inspect.json').read_text());mounts=[]
 for node in inspect:
  m=[m for m in node['Mounts'] if m['Destination']=='/data'];assert len(m)==1 and m[0]['RW'];mounts.append(m[0]['Source'])
 assert set(mounts)=={f'/tmp/js-wf-r5-retained-profile-restored-20261005/copied-stores/node-{n}' for n in range(5)}
 fixture=root/'originals'/test;comparisons=json.loads((fixture/'comparisons.json').read_text());expected={'Invocations':100000,'Journals':100000,'Entries':1200000,'Terminal':100000}
 for r in comparisons:assert r['Report']==expected and r['Error']=='<nil>' and 0<r['ElapsedNS']<20_000_000_000
 if label=='interrupted-cohort':
  assert [r['Label'] for r in comparisons]==['r5-byte-baseline','r5-byte-error','r5-byte-short']
  for r,starts,points in zip(comparisons,[[1],[1,129],[1,130]],[0,0,1]):
   assert r['Complete']
   inv,jrn=r['Phases'];assert inv['Stream']=='WF_INV' and inv['Records']==100000 and inv['CursorStarts']==[1] and inv['PointReads']==0 and inv['CursorReplicas']==[5]
   assert jrn['Stream']=='WF_JRN' and jrn['Records']==1200000 and jrn['CursorStarts']==starts and jrn['PointReads']==points and jrn['CursorReplicas']==[5]*len(starts) and jrn['Interrupted']==(len(starts)>1)
 else:
  assert [r['Label'] for r in comparisons]==['plain','traced'];snapshot=comparisons[1]['trace'];assert len(snapshot['Recent'])<=64
  for stream,count,size in [('WF_INV',100000,500000),('WF_JRN',1200000,84700000)]:
   c=snapshot['Counts'][stream+'.Next'];assert c['started']==c['completed']==count and c['errors']==0 and c['bytes']==size
   for op in ['Messages','CreateConsumer']:
    c=snapshot['Counts'][stream+'.'+op];assert c['started']==c['completed']==1 and c['errors']==0
 source=out/label
 for n in ['execution.json','binary.json','clone-input-verification.json','post-copied-store-hashes.json','native.log']:(source/n).write_bytes((root/n).read_bytes())
 (source/'comparisons.json').write_bytes((fixture/'comparisons.json').read_bytes())
 reports.append({'scope':label,'revision':before['revision'],'selected_go_module_inputs':len(before['files']),'all_inputs_match_git_and_before_after':True,'actual_sdk_file_process_hash_match':True,'five_server_copies_verified':True,'all_original_limits_hold':True,'elapsed_seconds':[r['ElapsedNS']/1e9 for r in comparisons]})
# The traced clone begins from the first campaign's closed post-run ledger.
first=Path('/tmp/js-wf-r5-continuous-interrupted-20261005');second=Path('/tmp/js-wf-r5-traced-cohort-20261005');assert json.loads((first/'post-copied-store-hashes.json').read_text())==json.loads((second/'clone-input-verification.json').read_text())['hashes']
(out/'qualification.json').write_text(json.dumps({'focused_full_cohort_r5_qualified':True,'five_complete_audits':True,'interrupted_exact_once_journal_visits':3600000,'trace_records':1300000,'trace_payload_bytes':85200000,'campaigns':reports,'scope':'Executed-source full retained cohort/reader and tracing controls, not original24h or full/final-source matrices'},indent=2)+'\n');(out/'executed-review.py').write_bytes(Path(__file__).read_bytes());print('QUALIFIED',reports,flush=True)
