import hashlib,json,pathlib,re
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-continuation-recovery-2026-10-09'
required={
 'final-metadata-race.log':['TestGraphContinuationRecoveryDecisions','TestGraphContinuationRecoveryRequiresIndex','TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation'],
 'native-loop-race.log':['TestNativeGraphContinuationHandoff'],
 'legacy-repair-race.log':['TestGraphReconcileLoopRetriesUncertaintyWithoutSkippingBoundary','TestGraphReconcileSignalCacheBindsInvocationGeneration','TestGraphReconcileRejectsMissingConfiguration','TestGraphReconcileFailedReadCannotCertifyCursorPrefix','TestGraphReconcileRetriesPublishedUnboundStart','TestGraphReconcileDefersHistoryPinsWhileDeliveryOwnsLease','TestGraphCanonicalStartRepairDuringPreparedAppend','TestGraphCanonicalStartScanDryRunUnknownAndPrefix','TestGraphCanonicalStartScanRetirementIsSupersededNotReenqueued','TestContinuationAnchorUsesHistoricalRuntimeFacts','TestWorkerGraphAdmissionRejectsIncompatibleCLI'],
 'bound-start-limit-race.log':['TestCanonicalBoundStartRepairRequiresExactSourceAndGeneration','TestCanonicalBoundStartRepairDoesNotDeduplicateRecovery','TestContinuationPreservesGlobalJournalLimitAndTerminalSlot'],
}
for name,tests in required.items():
 text=(base/name).read_text()
 assert not re.search(r'^.*--- (FAIL|SKIP):',text,re.M) and 'WARNING: DATA RACE' not in text,name
 assert not re.search(r'^FAIL',text,re.M),name
 for test in tests: assert re.search(r'^--- PASS: '+re.escape(test)+r' ',text,re.M),(name,test)
 assert re.search(r'^ok\s+js-wf/',text,re.M),name
model=(base/'final-metadata-race.log').read_text()
modes=['boundary','before-suspension','held','lease-unknown','source-forged','publish-before','publish-ack','partial-publish','authority-unknown','later-wait','terminal','retired','purging','wait-race','post-source-wait-race']
for mode in modes: assert f'--- PASS: TestGraphContinuationRecoveryDecisions/{mode} ' in model,mode
for kind in ('start','signal','terminal','continuation'):
 for seed in range(1,17): assert f'--- PASS: TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation/{kind}/{seed} ' in model,(kind,seed)
native=(base/'native-loop-race.log').read_text()
for name in ('R1','R3Domain'): assert f'--- PASS: TestNativeGraphContinuationHandoff/{name} ' in native,name
inputs=['client/graph_start.go','journal/graph_start_catalog.go','reconcile/graph_continuation.go','reconcile/graph_continuation_test.go','reconcile/graph_catalog_cycle_test.go','reconcile/graph_journal.go','reconcile/observations.go','worker/graph_continuation_test.go','cmd/wf-worker/graph.go']
review=dict(verdict='PASS',source_hashes_observed_at_review={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in inputs},log_hashes={p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in required},decision_cases=15,catalog_scanner_seed_cases=64,native_modes=['R1','R3Domain'],scope='Development continuation discovery/dispatch and internal stage components; public admission, compaction, materialization and full qualification remain open.')
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
