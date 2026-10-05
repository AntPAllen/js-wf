import pathlib,json,hashlib,tarfile,os
for kind in ['baseline','corrected','race']:
 root=pathlib.Path('/tmp/js-wf-result-absence-'+kind+'-20261005')
 before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text())
 assert before==after
 with tarfile.open(root/'source.tar.gz') as archive:
  members={m.name:m for m in archive.getmembers() if m.isfile()}
  assert set(members)==set(before)
  for name,want in before.items():assert hashlib.sha256(archive.extractfile(name).read()).hexdigest()==want
 sdk=json.loads((root/'sdk-live.json').read_text())
 assert hashlib.sha256((root/'integration.test').read_bytes()).hexdigest()==sdk['sha256']
 assert not pathlib.Path('/proc',str(sdk['pid'])).exists()
 native=(root/'native.log').read_text()
 execution=json.loads((root/'execution.json').read_text())
 assert execution['source_unchanged']
 assert execution['exit_code']==(1 if kind=='baseline' else 0)
 assert ('--- FAIL: TestResultReadVerifiesWeakAbsence' if kind=='baseline' else '--- PASS: TestResultReadVerifiesWeakAbsence') in native
 if kind!='baseline':
  for label in ['worker','client','deletion','corruption','permanent_stale_absence_deadline','blocked_leader_deadline_and_cancel','leader_rejects_malformed_metadata']:
   assert '--- PASS: TestResultReadVerifiesWeakAbsence/'+label+' ' in native
  assert native.count('real payload recovered; leader confirmations=1')==2
 fixture=root/'TestResultReadVerifiesWeakAbsence'
 retained={str(p.relative_to(fixture)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(fixture.rglob('*')) if p.is_file()}
 assert retained
 (root/'retained-native-files.json').write_text(json.dumps(retained,indent=2)+'\n')
 descriptors=[]
 for proc in pathlib.Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:fds=list((proc/'fd').iterdir())
  except OSError:continue
  for fd in fds:
   try:target=os.readlink(fd)
   except OSError:continue
   if target.startswith(str(fixture)+'/'):descriptors.append({'pid':proc.name,'fd':fd.name,'target':target})
 assert not descriptors,descriptors
 review={'kind':kind,'source_inputs':len(before),'source_before_after_matches':True,'every_source_archive_member_verified':True,'actual_sdk_sha256':sdk['sha256'],'sdk_terminal':True,'native_exit':execution['exit_code'],'retained_native_files':len(retained),'native_files_bytes':sum(v['bytes'] for v in retained.values()),'no_visible_native_file_descriptors':True,'injection':'synthetic missing metadata at client boundary; real R3 administrative oracle and payload; no native follower lag claim','scope':'worker result transport and client Await; snapshot, manifest and other object read paths unchanged'}
 if kind=='corrected':
  for filename,tests in [('existing-result-controls.log',['TestLargeStepResultSpillsAndReplays','TestLargeTerminalResultSpillsAndAwaitVerifies']),('result-read-budget-control.log',['TestWorkerResultMissingReplyRecovery/step_get_drop'])]:
   log=(root/filename).read_text()
   for test in tests:assert '--- PASS: '+test+' ' in log
  review['existing_result_controls_verified']=True
 (root/'executed-review.json').write_text(json.dumps(review,indent=2)+'\n')
 print(json.dumps(review))
