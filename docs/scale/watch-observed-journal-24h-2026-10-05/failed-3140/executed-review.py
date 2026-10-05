import pathlib,json,hashlib,tarfile,subprocess,shutil
repo=pathlib.Path('/home/exedev/js-wf')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for name,dest in [('watch-observed-journal-24h','watch-observed-journal-24h-2026-10-05/failed-3140')]:
 root=pathlib.Path('/tmp/js-wf-'+name+'-20261005'); out=repo/'docs/scale'/dest;out.mkdir(parents=True,exist_ok=True)
 proof=pathlib.Path('/tmp/js-wf-'+name+'-terminal-proof-20261005');proof.mkdir(exist_ok=True)
 execution=json.loads((root/'execution.json').read_text());assert execution['status']=='failed' and execution['test_exit_code']==1
 archive=json.loads((root/'archive-manifest.json').read_text()); assert sha(root/'originals.tar.gz')==archive['archive_sha256']
 checked=set()
 with tarfile.open(root/'originals.tar.gz') as t:
  for member in t:
   if not member.isfile():continue
   key=member.name.removeprefix('./'); f=t.extractfile(member);digest=hashlib.file_digest(f,'sha256').hexdigest()
   if key=='archive-manifest.json':continue
   assert key in archive['files'],key
   assert digest==archive['files'][key],key
   checked.add(key)
 assert checked==set(archive['files'])
 before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text());assert before==after and before['revision']==execution['source']
 for key,digest in before['files'].items():
  assert sha(root/'source'/key)==digest,key
  raw=subprocess.check_output(['git','show',before['revision']+':'+key],cwd=repo)
  assert hashlib.sha256(raw).hexdigest()==digest,key
 binary=json.loads((root/'binary.json').read_text()); assert sha(root/'integration.test')==binary['sha256']
 watch=pathlib.Path('/tmp/js-wf-watch-observed-journal-24h-live-20261005');observed=[json.loads(line) for line in (watch/'sdks.jsonl').read_text().splitlines()];parent=next(x for x in observed if x['pid']==162125);assert parent['sha256']==binary['sha256'] and not pathlib.Path('/proc/162125').exists()
 assert json.loads((watch/'watch-result.json').read_text())['parent_gone']
 for p in watch.iterdir():
  if p.is_file():shutil.copy2(p,proof/('watch-'+p.name))
 review={'execution':execution,'archive_sha256':archive['archive_sha256'],'original_members_read_and_verified':len(checked),'selected_source_inputs_bound_to_git_and_unchanged':len(before['files']),'sdk_sha256':binary['sha256'],'actual_observed_sdk_pid':162125,'actual_observed_sdk_sha256':parent['sha256'],'native_named_test_failed':True,'cause_unconfirmed':True,'no_native_rerun':True,'stores_not_reopened':True,'shared_vm_campaign_overlap':True}
 if True:
  traces={}
  for p in sorted((root/'fixture').glob('audit-batch-31[34]0*trace.json')):
   data=json.loads(p.read_text());traces[p.name]={'counts':data['counts'],'errors':[v for v in data['recent'] if v.get('error')]};shutil.copy2(p,proof/p.name)
  review['trace_comparison']=traces
  for n in ('failure-goroutines.txt','failure-goroutines.json','checkpoint-audits.json'):shutil.copy2(root/'fixture'/n,proof/n)
 else:
  watch=pathlib.Path('/tmp/js-wf-protobuf-json-worker-10m-live-20261005')
  for p in watch.iterdir():
   if p.is_file():shutil.copy2(p,proof/('watch-'+p.name))
 for n in ('execution.json','archive-manifest.json','binary.json','source-before.json','source-after.json','events.jsonl','test-environment.json'):
  shutil.copy2(root/n,proof/n)
 shutil.copy2(root/'originals.tar.gz',proof/'originals.tar.gz')
 (proof/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(proof/'independent-review.json',out/'independent-review.json')
 shutil.copy2(__file__,proof/'executed-review.py');shutil.copy2(__file__,out/'executed-review.py')
 subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(proof),str(out)],check=True)
 print(name,len(checked),len(before['files']),flush=True)
