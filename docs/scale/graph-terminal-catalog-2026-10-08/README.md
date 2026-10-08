# Bounded canonical terminal catalog recovery — 2026-10-08

The `graph-terminal` fenced repair loop now discovers canonical terminal generations whose `WF_STATE` key is absent, without caller IDs, input bodies or repair keys. It enqueues a delivery; the production worker validates canonical terminal ownership before rebuilding the projection. Present values, including purge markers, are skipped and are never accepted as terminal authority by the scanner.

`Client.RepairTerminalProjectionAttempt` checks the captured canonical token/generation/terminal kind, validates the actual bound source pointer and rechecks lifecycle before enqueue. It opens no input reader and does not mutate logical heads. Repair uses an empty message-ID key: an earlier recovery delivery may already be ACKed when the projection is deleted again, so native deduplication must not suppress the next recovery. Ambiguous enqueue returns unknown rather than silently retrying inside the client; the fenced scanner retains its uncertain cursor and retries discovery.

## Read-witness catalog churn

Quorum root reads publish authority witnesses and advance physical catalog sequences. A moving unbounded scan can keep consuming its own read witnesses without wrapping to revisit earlier destinations. The terminal scanner captures `RootCatalogHighWater`, retains that watermark across budgeted calls and uses `NextStartThrough` to defer records beyond the cycle boundary before their quorum read. It refreshes after wrap, changed cursor, error or restart. This watermark schedules work; it grants no payload or lifecycle authority. Existing Start/Signal scan behavior is unchanged.

The counterfactual overlay removing the watermark fails the explicit cycle-wrap control. The original fixture also compared cursor values across quorum reads; its failure is retained. Corrected controls certify only the confirmed prefix and preserve uncertainty, while allowing physical sequence movement.

## Evidence

- Twelve shared production-path modes pass 1,000 generated schedules with exact replay in normal and race mode: healthy, lookup/catalog/watermark uncertainty, dropped/lost-ack enqueue, repeated projection deletion, dry-run, failed terminal, present mirror, purge marker and retired generation.
- The original dispatch is acknowledged and physically drained before discovery. The scanner is the sole source of later dispatch. Recovery reconstructs exact terminal bytes, runs no handler, changes no foreign lease or journal, and completely drains dispatch and fixture graph objects. A second deletion recovers inside the message-ID dedup window.
- Native R1/R3 normal/race controls run the actual fenced `graph-terminal` loop with reopened stores, no retained dispatch and two projection deletions. Exact terminal bytes and journal tail/records remain unchanged; the effect count stays one.
- Bounded/dry-run/unknown-prefix/retirement/read-witness wrap controls pass normal/race. Twelve new corpus pins retain each mode; all 736 previous pins remain byte-identical. Inventory now has 148 seeded families and 748 pins.
- `development/` retains command results, the failed fixture and unbounded negative control, pin hashes and executed coverage/source-lineage review. These are component development results, not complete frozen current-suite qualification.

## Remaining scope

Projection lookups use legacy KV observations; read staleness can delay or duplicate discovery and never grants result authority. Metadata/source rechecks do not provide an atomic cross-stream purge/enqueue/state publication fence. Present forged/stale/corrupt projections remain outside this absent-key recovery. Catalog scanning still requires canonical Start metadata and a retained matching invocation source.

Full invocation/state/timer/tombstone/snapshot/continuation/import/history/projection-consumer/CLI/deployment migration, arbitrary fault combinations/permutations, extended and complete current suites, all original native process/storage/power-loss/concurrency/capacity/matrices/actual24h/million physical-drain/default-adoption/release requirements remain open. Fixture collection does not enable production online GC. The accepted normal 147-family suite and live race 146-family campaign retain their original frozen-source scopes.
