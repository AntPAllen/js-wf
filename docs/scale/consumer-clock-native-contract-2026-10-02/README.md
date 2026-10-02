# Native consumer pending-clock contract and seeded replay

The opt-in `TestFiveContainerConsumerPendingClockTransition` runs pinned NATS
2.15.0 with five Docker servers, R5 File WorkQueue storage and an R5 durable
consumer. It verifies actual ±60-second physical clocks, confirms the shifted
consumer leader, processes progress and delayed NAK, confirms three pending
entries, then SIGKILLs that leader. An unshifted replacement redelivers exactly
those three entries once. A confirmed-ACK control never redelivers; final
confirmed ACKs drain both consumer state and physical stream storage.

The corrected run passes in 101.80 seconds (102.818 seconds package time).
Ahead redeliveries occur after 72.999 seconds for initial ACK-wait, 73.003 for
progress, and 65.003 for a five-second delayed NAK. Behind redeliveries occur
about 6.76 seconds after delivery, following the election. Stored message
timestamps are independently checked against raw stream retrieval. Physical
clocks, consumer states, samples, handoff, server logs, stores and source hashes
are retained under `stored-pending-contract/`; 513 original files are compressed
and hashed. The authoritative service is inactive/success/exit zero.

## Corrected timestamp contract

Pinned server `consumer.go` delivers by replicating `pmsg.ts` before tracking
active pending state using consumer wall time. `filestore.go` persists that
replicated timestamp; replacement leadership restores it. Progress replaces it
with the consumer clock. Delayed NAK replaces it with consumer time minus
AckWait plus delay. Therefore initial restored ACK-wait depends on the stored
message clock, while subsequent updates depend on the consumer clock.

The earlier `initial-prediction-mismatch/` run completed and physically drained
both cases but failed its prediction that initial pending state always uses the
consumer clock. Its named ephemeral consumer and healthy stored timestamps
are retained as counterevidence, not rewritten as a durable-consumer pass.
The corrected run uses durable consumers; its stored timestamps happened to
come from the shifted stream leader. These are six native cases, not every
independent combination of stream and consumer clocks.

## Deterministic regression scope

The new opt-in stored-pending contract preserves legacy trace semantics. The
production partition dispatch loop runs 18 combinations: consumer ±60 seconds,
stored message clock ahead/behind/healthy, and ACK-wait/progress/five-second NAK.
It restores replicated deadlines on leadership transfer, requires exactly two
deliveries and final physical removal, and supports exact replay. All 1,000 seeds
and the legacy workload pass under race in 1.457 seconds; 18 new pins plus six
legacy clock pins replay in 1.096 seconds. The total corpus is 252 pins.
Independent clock combinations extend the model beyond the six durable native
cases; the model does not simulate Raft election or update commitment.

This verifies a broker mechanism compatible with the retained ahead-clock drain
failure. It does not prove which ACK/NAK updates committed in that earlier run,
clear that failed workflow row, or relax its existing latency/drain limits.
