import hashlib,json,pathlib,re
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-bounded-continuation-2026-10-09'
required={
 'final-prefix-race.log':['TestNativeGraphCheckpointMaterializedReferences'],
 'bounded-native-race.log':['TestNativeGraphCheckpointMaterializedReferences','TestNativeGraphContinuationHandoff','TestNativeGraphUnpublishedContinuationHandoff'],
 'sdk-signal-race.log':['TestNativeGraphContinuationSDKFlow','TestNativeGraphContinuationSDKPartitionFlow','TestNativeGraphContinuationBufferedSignals','TestContinuationPreservesGlobalJournalLimitAndTerminalSlot'],
 'final-binding-race.log':['TestNativeGraphContinuationBufferedSignals','TestNativeGraphContinuationChildPromise','TestNativeGraphContinuationBufferedChildPromise'],
 'child-signal-regressions-race.log':['TestGraphChildReplayRejectsForgedProvenance','TestGraphSelectedChildRequiresExactRecordedSignal','TestNativeCanonicalSignalQueueWorkerReplay'],
}
for log,tests in required.items():
 text=(base/log).read_text()
 assert not re.search(r'^.*--- (FAIL|SKIP):',text,re.M) and 'WARNING: DATA RACE' not in text,log
 assert not re.search(r'^FAIL',text,re.M),log
 assert re.search(r'^ok\s+js-wf/worker',text,re.M),log
 for test in tests:
  assert re.search(r'^--- PASS: '+test+' ',text,re.M),(log,test)
  if not test.startswith('TestNativeGraph'):continue
  for mode in ('R1','R3Domain'):assert f'--- PASS: {test}/{mode} ' in text,(log,test,mode)
assert (base/'final-prefix-race.log').read_text().count('promise aliases=2 completion_payload_edges=3 materialized_index=132 blocked_prefix_reads=0')==2
assert (base/'final-binding-race.log').read_text().count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records=25 result=43')==4
assert (base/'final-binding-race.log').read_text().count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records=26 result=43')==2
inputs=['journal/graph_signal_queue.go','worker/graph_journal.go','worker/graph_checkpoint.go','worker/worker.go','worker/graph_signal_queue.go','worker/graph_checkpoint_test.go','worker/graph_continuation_test.go','worker/graph_continuation_sdk_test.go']
review=dict(verdict='PASS',source_hashes_observed_at_review={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in inputs},log_hashes={p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in required},native_modes=['R1','R3Domain'],blocked_encoded_prefix_entries=131,resume_anchor_index=132,reconstructed_records=1,reconstructed_refs=3,scope='Development bounded resume and materialized worker provenance with native SDK/signal/child controls. Physical compaction/collection, process-kill and complete qualification remain open.')
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
