import pathlib,json,hashlib,tarfile,os
for kind in ['baseline','corrected','race']:
 root=pathlib.Path('/tmp/js-wf-snapshot-leader-'+kind+'-20261005')
 before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text())
 assert before==after
 with tarfile.open(root/'source.tar.gz') as archive:
  members={m.name:m for m in archive.getmembers() if m.isfile()}
  assert set(members)==set(before)
  for name,want in before.items():assert hashlib.sha256(archive.extractfile(name).read()).hexdigest()==want
 sdk=json.loads((root/'sdk-live.json').read_text())
 assert hashlib.sha256((root/'integration.test').read_bytes()).hexdigest()==sdk['sha256']
 assert not pathlib.Path('/proc',str(sdk['pid'])).exists()
 native=(root/'native.log').read_text();execution=json.loads((root/'execution.json').read_text())
 assert execution['source_unchanged'] and execution['exit_code']==(1 if kind=='baseline' else 0)
 assert ('--- FAIL: TestSnapshotManifestReadsUseLeader' if kind=='baseline' else '--- PASS: TestSnapshotManifestReadsUseLeader') in native
 if kind!='baseline':
  for label in ['absent','old','object_absence','deleted_and_purged','malformed_manifest','blocked_leader_deadline_cancel']:
   assert '--- PASS: TestSnapshotManifestReadsUseLeader/'+label+' ' in native
  assert native.count('200 exact records; committed manifest revision=2 leader requests=2 direct requests=0')==2
 else:
  assert 'expected index 0, got 184' in native and 'expected index 104, got 184' in native
 fixtures=sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith('Test'))
 retained={str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for fixture in fixtures for p in sorted(fixture.rglob('*')) if p.is_file()}
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
   if any(target.startswith(str(fixture)+'/') for fixture in fixtures):descriptors.append({'pid':proc.name,'fd':fd.name,'target':target})
 assert not descriptors,descriptors
 review={'kind':kind,'source_inputs':len(before),'source_before_after_matches':True,'every_source_archive_member_verified':True,'actual_sdk_sha256':sdk['sha256'],'sdk_terminal':True,'native_exit':execution['exit_code'],'retained_native_fixtures':[p.name for p in fixtures],'retained_native_files':len(retained),'native_files_bytes':sum(v['bytes'] for v in retained.values()),'no_visible_native_file_descriptors':True,'injection':'synthetic weak manifest absence/old revision and object absence; native R3 compactions, administrative leader requests and payload reads; no natural follower lag claim','scope':'default journal snapshot manifest and object read adapters; snapshot CAS/purge/state machine budgets unchanged'}
 if kind=='corrected':
  log=(root/'existing-compaction-controls.log').read_text()
  for test in ['TestResultReadVerifiesWeakAbsence','TestSnapshotPurgeLeaseBoundaryAgainstRealCluster','TestConcurrentSnapshotCompactorsAgainstRealCluster','TestReadRetriesSnapshotMovedDuringLiveScan','TestSnapshotKeepsConcurrentAppendBeforePurgeAgainstRealCluster/append_drop','TestSnapshotKeepsConcurrentAppendBeforePurgeAgainstRealCluster/append_ack_lost']:
   assert '--- PASS: '+test+' ' in log,test
  assert 'snapshot_purge_lease_contract_test.go:149: nats: no responders available for request' in log
  assert '--- FAIL: TestSnapshotKeepsConcurrentAppendBeforePurgeAgainstRealCluster/clean ' in log
  rerun=(root/'clean-append-body-control.log').read_text()
  assert '--- PASS: TestSnapshotKeepsConcurrentAppendBeforePurgeAgainstRealCluster/clean ' in rerun
  unit=(root/'journal-unit-controls.log').read_text()
  assert 'js-wf/journal' in unit and 'js-wf/internal/natsutil' in unit and 'FAIL' not in unit
  review.update(existing_controls='successful named bodies checked; original group remains failed because clean setup PutBytes had no responders; exact clean body replay passes',existing_unit_controls_verified=True,temporary_existing_fixture_limitation='ordinary existing control stores deleted by their fixture cleanup; retained result-regression fixture and primary snapshot fixture included; additional control process live identities not captured')
 (root/'executed-review.json').write_text(json.dumps(review,indent=2)+'\n')
 print(json.dumps(review))
