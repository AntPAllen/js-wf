# Worker durable archive maintenance — 2026-10-10

Workers using an explicitly configured compaction descriptor port now persist
completed staging batches and pending renewal input. They renew ownership before
each save/delete and scope batch. Unknown writes stop the delivery; a fresh
worker loads the revision that actually committed. Verification remains private
and restarts in full. Successful publication deletes only the observed revision.
A lost delete is repaired from canonical completion before stage execution.

The delivery reader is released after confirmed suspension, before capturing
portable source authority. Its uncertain release is sticky across cleanup.
Recovery observes the canonical checkpoint/suspension boundary before acquiring
another reader, verifies the invocation pointer matches canonical Start, and
resumes the saved original-head operation. After maintenance it dispatches a new
delivery; the current delivery never executes through relocated old references.

An observed stale or expired descriptor is deleted under a fresh lease check and
that delivery returns Stale. A later delivery can start a fresh stage/token;
there is no refreshed-head final CAS or revival of expired grants. Unknown reads
and corrupt incomplete input remain errors. Once the exact canonical archive is
complete, descriptor bytes are obsolete even if corrupt: cleanup targets their
observed revision before opening the next stage reader. No production admission
flag or default configuration is changed.

## Evidence

The restored race selection passes36.317s:40 JSON/protobuf stored-delivery
controls,3 reader-release controls,32 existing maintenance controls,7 archive
repair controls and2 native domain cases. Source hashes and compressed bytes
freeze nine inputs; `review.py` checks terminal results and scope.

Stored controls include stage/record/node/renewal cuts, dropped/lost create/update/
delete replies, lost reads, external reader source changes, expiry, corrupt data,
a mismatched invocation, cancellation and owner loss before save/delete. Every
case checks the canonical retention boundary and reader cleanup. Healthy paths
execute initial/next stages once and leave no descriptors. Stage cuts save2 source
records and resume with5 staging batches. Verification cuts resume the completed
stage in1 transition batch and require all10 verification batches. Renewal cuts
reload the pre-renewal input/requested expiry, run12 renewal scope batches, cross
the old deadline and then complete staging/verification. Renewal fixtures share
the modeled clock with leases; their120-second lease separates that deliberate
41-second intent-clock advance from ownership loss. These are controlled-time
functional controls, not recovery latency measurements.

Reader-release controls distinguish dropped-before and committed-without-ack
outcomes plus a lost readback. Cleanup issues no hidden read/mutation and returns
the original uncertain error. Dropped release retains1 reader until normal expiry;
committed release retains0. Cancellation before a release attempt still permits
independent cleanup. The first development controls failed because the model's
empty Keys result uses ErrNoKeysFound; that log is retained outside acceptance.

Native R1/R3 cases use actual graph/object stores, native invocation/lease/dispatch
ports and separately provisioned file-backed descriptor KV. Three newly
constructed workers/graph adapters execute the cut, recovery, and next stage.
They independently compare the saved source head with canonical authority after
closing the first worker, require staging counts[1,5,0] and verification[0,10,0],
and confirm reader/descriptor cleanup and one call per stage. The server stays
running. Cancellation and fresh construction exercise local state loss, not an
OS process kill, peer restart, VM/power loss or collector race. The existing
2-minute native fixture parent and15-second publication/recovery parents remain.

Seven deliberate bypasses fail2/2/2/8/2/2/2 selected controls: omit delivery
release, skip pre-open recovery, discard saved prefix, swallow unknown save,
ignore owner loss at save, ignore owner loss at delete, and retry uncertain
reader release. Exact mutant bytes, commands/results and logs are saved. Every
source is restored before the combined positive race run. All853 common pins
match their saved traces on the restored source (4.131s).

```sh
go test -race ./worker -run '^(TestGraphDeliveryCompactionReleaseUncertaintyIsSticky|TestGraphContinuationStoredMaintenanceAcrossDeliveries|TestGraphContinuationMaintenanceLeaseAndCancellation|TestGraphContinuationRepairsArchiveBeforeStage|TestNativeGraphStoredMaintenanceFreshWorkerRecovery)$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-worker-durable-maintenance-2026-10-10/review.py
```

## Remaining work

The whole15-second handoff limit still covers the first complete checkpoint scan
and final private verification. Staging can now survive a delivery cut, but those
private scans cannot make cross-process certified progress. Make healthy bounded
maintenance continue under renewed ownership beyond that whole-handoff limit,
then qualify actual100000-entry/native worker termination and the original full
campaign/retention/rollout gates. Public graph continuation admission remains
closed, production collection remains off, and the prior actual100000 failure
and majority/soak failures remain open. Historical descriptor-only qualifications
remain scoped to their frozen sources.
