# Durable graph handoff inherits its delivery context — 2026-10-10

For graph archive stores with an explicitly configured compaction descriptor port,
publication and saved-maintenance recovery now inherit their delivery context.
There is no additional15-second deadline across the entire handoff. Caller
cancellation/deadlines still apply; every verification/staging batch retains its
15-second child context and unconditional owner renewal. Descriptor saves/deletes
and ownership requests use3-second children, complete-descriptor reads3 seconds,
and final dispatch5 seconds. The constructor/resume operation remains bounded15
seconds. Cleanup remains separately bounded. Legacy and ephemeral graph handoffs
retain their previous whole15-second deadline; public admission stays closed.

## Evidence

The restored race selection passes78.221s:4 context-policy controls,42 stored
JSON/protobuf delivery controls,3 uncertain-release controls,32 existing
maintenance controls,7 archive-repair controls and native R1/R3 fresh-worker cases.
Two JSON wall-time controls spread three6-second pauses across separately bounded
operations. Initial publication and saved-progress recovery each complete after
more than18 seconds. These pauses occur after successful batch/request completion;
they do not relax a blocked RPC or claim latency qualification. The modeled lease
clock remains controlled; these exercise Go's real context deadline policy.

Policy controls preserve the original caller deadline/cancellation, reject an
already expired caller, keep legacy/ephemeral/nonarchive timing, and avoid
canceling the parent during cleanup. Descriptor ports reject requests without
individual deadline bounds. Restoring the whole15-second cap fails both wall-time
controls. Detaching the caller fails the durable policy control. Removing the
complete-descriptor read's child bound fails both normal encoding controls.
After that final bypass, all42 stored and4 policy controls pass again (49.097s). Exact mutant
bytes, commands/results, hashes and compressed source bytes are retained.

The unchanged64-entry private-cap terminal-slot fixture can now select this
configuration with WF_GRAPH_CONTINUATION_DURABLE=1. It provisions matching-replica
file-backed LIMIT_PROGRESS KV, including through the existing profile decorator.
Native R1/R3 archive cases pass under race97.609s with real SDK padding/checkpoints,
terminal reservation and zero rejected effects. This is budget64, not a
100000-entry run. The production default cap remains100000 and the functional
fixture's2-minute watchdog is unchanged. All853 common pins pass8.695s.

```sh
go test -race ./worker -run '^(TestContinuationPublicationContextOwnershipAndBounds|TestGraphContinuationStoredMaintenanceAcrossDeliveries|TestGraphDeliveryCompactionReleaseUncertaintyIsSticky|TestGraphContinuationMaintenanceLeaseAndCancellation|TestGraphContinuationRepairsArchiveBeforeStage|TestNativeGraphStoredMaintenanceFreshWorkerRecovery)$' -count=1 -v
WF_GRAPH_CONTINUATION_LIMIT_BUDGET=64 WF_GRAPH_CONTINUATION_DURABLE=1 go test -race ./worker -run '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R(1|3)/archive=true$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-worker-publication-parent-2026-10-10/review.py
```

## Scale work still open

The previous actual100000 failure remains a failure; no new full-cap execution
has been accepted. Renewal enumerates all isolated namespace scope keys and
checks each before extending owned grants. Its setup is context-bounded but not
per-item memory-bounded; its total renewal must finish before the original intent
expires. NativeAuthority.BlobKeys obtains a complete subject census. This work
therefore grows with the namespace, including unrelated/foreign scopes. Its
cost at the full-cap namespace is unqualified and should be addressed/qualified
before spending another multi-hour padding run. Removing the parent deadline
alone does not prove full-cap liveness, server-side causes, worker termination,
current full seed campaigns, original majority/soak/retention or rollout gates.
Production collection remains off and public continuation admission remains
closed. Historical evidence remains scoped to its frozen source.
