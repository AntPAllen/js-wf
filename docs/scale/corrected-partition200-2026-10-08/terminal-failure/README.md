# Corrected full200 campaign stopped at seed2

The original frozen2ef3e8b campaign stopped on its first failure: SDK2818176 fails634.30s at the unchanged per-type terminal p99 gate. All1988 expected invocations are included in both terminal latency and retained reports; the native retained report is1988 invocations/journals/terminal records and21941 entries. The failing cell is71 fanout invocations, p99=30.188460653s. Independent raw timestamp review verifies all9372 samples,1988 terminal samples,19 exact majority-isolation fault identities and the offending sample `seed-2-batch-5-4`. Seed1 remains separately accepted; seeds3–200 did not execute. Original wrapper/producer/SDK/three-peer processes are closed and both seed source/SDK/candidate/input ledgers are retained.

The outlier was enabled01:35:27.507155061Z and observed01:35:57.695615714Z. It overlaps the first minority-node partition (majority probe acknowledged); confirmed heal01:35:51.019353375Z gives6.676262339s diagnostic post-heal delay. The current plan retains the raw gate for majority-progress partition tests; the quorum-loss route exception does not apply. No threshold change, acceptance by alternate metric or native retry is made. Dispatch/operation projections show lease-held duplicate deliveries followed by redelivery; they do not establish the cause of the scheduling gap. The complete original retains all histories, latency/fault/dispatch/operation logs and native disk stores. No NATS broker/store was reopened, and native retained counts are not an independent disk reconstruction.

[Independent failure review](independent-failure-review.json), [offending dispatch projection](outlier-dispatch.json), [operation projection](outlier-operations.json), [complete archive manifest](archive-verification.json). The graph test unit began01:48:24UTC, after this outlier and after the native campaign's01:45:31 failure; the graph scale test did not cause the failing sample. That unit's original start timestamp is independently retained in the [graph review](../../retained-append-graph-2026-10-08/accepted/review.json).

Full candidate200 remains failed and unqualified. Complete default dependency, original native matrices/24h/million physical drain, canonical runtime migration and onlineGC requirements remain open. Diagnose this original trace or a bounded source-bound reproduction before another full campaign. Original full124 normal qualifier continues unchanged.

## Dispatch gap analysis — 2026-10-10

The [reproducible analysis](analyze-dispatch-gap.py) verifies the frozen Git
configuration and retained projection identities, using integer nanoseconds.
[Computed results](dispatch-gap-review.json) show source defaults of 12s lease
TTL, 13s AckWait and 5s held-lease NAK delay; the in-process matrix worker has no
AckWait override. This is not the earlier worker-kill 30s TTL mismatch.

The largest invocation dispatch gap is 30.123950250s, from acknowledgment of
run615 to fetch of run617 delivery2. Run617's first NAK returned no client error;
its expected five-second deadline fell 394.960143ms after partition start.
Actual redelivery followed that NAK by 30.169510428s and confirmed heal by
6.620203965s. The longest recorded operation was journal_read at 29.161366ms.
These invocation projections do not measure partition-wide slot occupancy,
pull lifetime, server redelivery deadlines or whether the broker processed the
NAK. They therefore locate the observed gap without establishing its cause.
The next bounded reproduction needs those observations before another full
campaign. Raw p99 remains failed; no gate change or native rerun was made.

### Optional partition observations

`worker.WithPartitionObserver` now records begin/end pairs for local slot waits
and complete pulls, including batch-channel waiting and cancellation. Each pair
carries worker, partition, per-loop attempt, duration, concurrency and reserved
slot count. Reservations include the current pull, so they must not be reported
as an active-handler count. With concurrency1, the full pull also includes
synchronous handler execution. Attempt numbers are local to a RunPartition call;
they are not persistent consumer delivery identifiers. Callbacks must be quick
and concurrency-safe.

Set `WF_MATRIX_PARTITION_TIMINGS=1` for an **in-process** mixed matrix worker to
save `MATRIX_ARTIFACT_PREFIX-partition.jsonl`. This is separate from operation
timings and remains disabled by default. Process-worker rows do not attach this
observer. No server receipt/deadline evidence is implied by these local events.

Development verification: race controls for full-slot and stalled-batch
cancellation pass (`TestPartitionObserverSeparatesSlotWaitAndPull`, including
the public worker transport option), as do existing partition cancellation and
native fetch-context controls. Both existing dispatch simulation families pass
1000 generated seeds each with exact replay under race (4.741s). The integration
package compiles with `-run '^$'`; that command executes no matrix tests. No new
native partition outcome or whole-plan qualification is claimed.

The complete failed campaign, archive and registered clean source worktree are now S3-preserved and retired after fresh full remote byte/member verification, unchanged inventories and closure checks. Further store inspection requires a fresh restore. [Corrected storage accounting and removal ledger](../reclaimed/removal.json) records the original hardlink double-count and observed951947264-byte filesystem free-space increase; no exact per-fixture physical reclaim is claimed.
