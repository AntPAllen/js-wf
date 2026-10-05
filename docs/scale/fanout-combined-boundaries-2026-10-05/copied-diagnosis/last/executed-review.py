from pathlib import Path
import json,hashlib,subprocess,shutil
repo=Path('/home/exedev/js-wf');donor=Path('/tmp/js-wf-fanout-combined-boundaries-20261005');de=json.load(open(donor/'execution.json'));assert de['status']=='failed' and not Path(f"/proc/{de['pid']}").exists()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for position in ['first','interior','last']:
 r=Path('/tmp/js-wf-fanout-copied-diagnosis-'+position+'-20261005');out=repo/'docs/scale/fanout-combined-boundaries-2026-10-05/copied-diagnosis'/position;out.mkdir(parents=True,exist_ok=True)
 e=json.load(open(r/'execution.json'));b=json.load(open(r/'binary.json'));assert e['status']=='passed' and e['exit_code']==0 and not Path(f"/proc/{e['pid']}").exists();assert sha(r/'review-sdk')==e['actual_sha256']==b['sha256'] and e['actual_build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
 before=json.load(open(r/'copy-before.json'));after=json.load(open(r/'original-after.json'));assert before['all_initial_copy_bytes_match'] and after['unchanged'] and before['files']==after['original_closed_files'] and before['original_source']==de['source'] and before['original_sdk_sha256']==de['sha256']
 for n,d in before['files'].items():assert sha(Path(before['original_root'])/n)==d
 inputs=json.load(open(r/'selected-inputs.json'));assert inputs['production_source']==de['source'];template=subprocess.check_output(['git','show',inputs['helper_git_source']+':scripts/fanout-retained-diagnosis.go.txt'],cwd=repo);assert hashlib.sha256(template).hexdigest()==inputs['helper_git_sha256']==sha(r/'helper.go')
 bound=json.load(open(donor/'source-before.json'))['files'];external=json.load(open(donor/'external-source-before.json'))
 for name,d in inputs['files'].items():
  assert sha(r/d['captured'])==d['sha256'];path=Path(name)
  if path.is_relative_to(repo):assert bound[str(path.relative_to(repo))]==d['sha256']
  else:assert external[name]==d['sha256']
 for n,d in inputs['module_files'].items():assert sha(r/'selected-source'/n)==d==bound[n]
 p=json.load(open(r/'retained-review.json'));assert len(p['children'])==len({c['id'] for c in p['children']})==500
 bad=[c for c in p['children'] if c.get('tail',{}).get('kind') not in ['Completed','Failed']];assert len(bad)=={'first':4,'interior':4,'last':7}[position];assert all(c['partition']!=52 for c in bad)
 tail=p['parent_records'][-1];assert tail['kind']=='Suspended';waiting=tail['payload']['waiting_on'];requests={x['payload']['name']:x['payload']['child_id'] for x in p['parent_records'] if x['kind']=='StepRequested' and x['payload'].get('kind')=='call_async'};assert requests[waiting.removeprefix('signal:')] in {c['id'] for c in bad}
 native=donor/'originals'/'TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix'/'results'/position
 if (native/'actual-parent-sdk.json').exists():
  prefix=json.load(open(native/'actual-parent-sdk.json'))['durable_prefix'];assert p['parent_records'][:len(prefix)]==prefix
 pending=[{'name':c['name'],'pending':c['num_pending'],'ack_pending':c['num_ack_pending']} for c in p['consumers'] if c['num_pending'] or c['num_ack_pending']]
 review={'position':position,'original_source':de['source'],'actual_copied_diagnosis_sdk_sha256':e['actual_sha256'],'helper_git_source':inputs['helper_git_source'],'helper_git_sha256':inputs['helper_git_sha256'],'selected_inputs_verified':len(inputs['files']),'original_closed_files_unchanged':len(before['files']),'parent_partition':52,'parent_tail':tail,'child_requests':500,'child_terminals':p['child_terminal_count'],'incomplete_outside_children':bad,'parent_waits_on_an_incomplete_outside_child':True,'original_captured_prefix_unchanged_in_copy':position!='last','pending_consumers':pending,'read_review_ns':p['review_ns'],'qualifies_original_recovery':False,'limitations':['Read-only fresh copy observations after shutdown; not original server-fault causality.','Original stores not reopened.','Copied streams observed through node0; not full integrity or all-peer physical drain.','Native parent matrix remains failed.']}
 (r/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,r/'executed-review.py')
 for n in ['independent-review.json','retained-review.json','execution.json','executed-producer.py','executed-review.py']:shutil.copy2(r/n,out/n)
 print(position,len(bad),len(before['files']),len(inputs['files']),flush=True)
 subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(r),str(out)],check=True)
