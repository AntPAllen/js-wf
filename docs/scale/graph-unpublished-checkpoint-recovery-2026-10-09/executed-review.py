import hashlib,json,pathlib,re
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-unpublished-checkpoint-recovery-2026-10-09'
required={
 'model-race.log':['TestGraphContinuationRecoveryDecisions','TestGraphContinuationRecoveryRequiresIndex','TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation','TestCanonicalBoundStartRepairRequiresExactSourceAndGeneration','TestCanonicalBoundStartRepairDoesNotDeduplicateRecovery'],
 'native-race.log':['TestNativeGraphContinuationHandoff','TestNativeGraphUnpublishedContinuationHandoff'],
 'final-sdk-flow-race.log':['TestNativeGraphContinuationSDKFlow','TestContinuationPreservesGlobalJournalLimitAndTerminalSlot'],
 'sdk-partition-race.log':['TestNativeGraphContinuationSDKPartitionFlow'],
}
for name,tests in required.items():
 text=(base/name).read_text()
 assert not re.search(r'^.*--- (FAIL|SKIP):',text,re.M) and 'WARNING: DATA RACE' not in text,name
 assert not re.search(r'^FAIL',text,re.M),name
 for test in tests: assert re.search(r'^--- PASS: '+re.escape(test)+r' ',text,re.M),(name,test)
 assert re.search(r'^ok\s+js-wf/',text,re.M),name
model=(base/'model-race.log').read_text()
modes=['boundary','before-suspension','held','lease-unknown','source-forged','publish-before','publish-ack','partial-publish','authority-unknown','later-wait','terminal','retired','purging','wait-race','post-source-wait-race']
no_pointer=['boundary','held','lease-unknown','source-forged','publish-before','publish-ack','later-wait','wait-race','post-source-wait-race','ordinary','completion-race','pointer-race']
for mode in modes+['unindexed-'+m for m in no_pointer]: assert f'--- PASS: TestGraphContinuationRecoveryDecisions/{mode} ' in model,mode
for kind in ('start','signal','terminal','continuation'):
 for seed in range(1,17): assert f'--- PASS: TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation/{kind}/{seed} ' in model,(kind,seed)
for log,tests in required.items():
 if log=='model-race.log': continue
 text=(base/log).read_text()
 for test in tests:
  if not test.startswith('TestNativeGraph'): continue
  for mode in ('R1','R3Domain'): assert f'--- PASS: {test}/{mode} ' in text,(log,test,mode)
for log in ('final-sdk-flow-race.log','sdk-partition-race.log'):
 assert (base/log).read_text().count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records=20 result=43')==2,log
inputs=['client/graph_start.go','journal/graph_start_catalog.go','reconcile/graph_continuation.go','reconcile/graph_continuation_test.go','worker/graph_continuation_test.go','worker/graph_continuation_sdk_test.go']
review=dict(verdict='PASS',source_hashes_observed_at_review={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in inputs},log_hashes={p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in required},decision_cases=27,new_no_pointer_cases=12,catalog_scanner_seed_cases=64,native_modes=['R1','R3Domain'],scope='Development metadata recovery, published/unpublished handoff, direct SDK workflow and native partition scheduling. Process-kill, compaction/materialization and full qualification remain open.')
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
