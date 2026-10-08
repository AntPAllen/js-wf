# Independently indexed canonical graph forests — 2026-10-08

Frozen source `647c933771be98b024119b88a3d5b82279b199ab` passes all six verification commands. Every one of the 1,604 selected tracked Go/YAML/module/pin inputs matches frozen Git before, after and at independent review. Every command uses count1, two Go CPUs and512MiB. The100,000-schedule normal simulation's ten-minute bound was selected before launch from the measured fixture cost; other commands retain five-minute bounds. No failed gate was relaxed or rerun.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
| ownership-normal | 32.842 | 58 |
| journal-normal | 7.522 | 14 |
| sim-normal | 324.018 | 3 |
| ownership-race | 112.655 | 58 |
| journal-race | 46.49 | 14 |
| sim-race | 62.633 | 3 |

## What changed

The permanent `streams-v4` authority root contains up to four sorted, independently indexed named append forests beside the original unnamed forest. These are logical retained graphs, not additional JetStream streams. Preparing an append selects one forest, registers every exact node/payload location with its name, and preserves all other forests. Commit validates that preservation and publishes the append plus bounded application descriptor at the original shared root head. Competing publication, application fencing, whole retirement, reader metadata and expired-intent collection invalidate stale plans through that same CAS.

The collector resolves intent locations only in their named forest. A receipt at the same numeric coordinate in another forest is insufficient. Same-forest owned payload reuse retains the original exact grant; cross-forest reuse is rejected and currently requires copying bytes into a fresh grant. Reader pins capture the complete graph set, v2 reader checkpoints fingerprint both the original forest and named forests, and retained reads validate the exact canonical pin. Existing v1 reader checkpoints remain valid for their original snapshots after a destination upgrades. Whole retirement clears every live forest together and preserves pinned snapshots, lifecycle bytes and permanent schema/head.

Legacy root/location/checkpoint bytes omit all new fields. Every654 existing pin remains byte-for-byte unchanged and passes in both simulation commands. Sixteen new pins cover the new family. Inventory is **142 workloads/670 pins**. CI includes the new family alongside the existing16 rows; hosted CI completion is not inferred.

## Verified controls

The complete graphpublication package passes **58 groups normal/race**, including six new deterministic unit groups and native R1/R3 controls. Native fixtures store840,000-byte identical payloads under separate input/signal grants, append an independent journal record, acquire a complete reader pin, fence a staged signal by retirement, reopen adapters from committed native state, resume the durable checkpoint, retain exact old bytes across collection, then release and verify physical object/chunk drain. They also reject all schema downgrades after retirement. Adapter reopening is not process-crash qualification.

Unit controls cover17 records in each named forest and the original forest, same-forest reuse without recopy, independent indexes, immutable copies, old readers across upgrade, forged checkpoints, cross-forest and false-source references, changed inherited forests, wrong stream coordinates, unknown collection reads without deletion, exact lost-reply readback, all shared-head winners and bounded/canonical schema validation.

The new shared transport family completes **100,000 normal** and **1,000 race** schedules, with exact replay, actual completed-body accounting and every16 mode covered. Its independent raw census validates nodes, payload bytes, physical identities, origin grants and stream-qualified coordinates in all live and pinned forests. Modes cover ordinary append, dropped/lost append and retirement, unknown readback, competing forests, purge/retirement/collector wins, renewal/expiry, forged checkpoint, wrong coordinates, independent copies and downgrade rejection. All670 unique regression pins pass in both commands. The existing14 graph journal groups also pass normal/race.

- [Independent source/event/inventory review](review.json)
- [Commands, environments and timings](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Preserved development failure and controls](../graph-streams-2026-10-08-development/)

## Runtime adoption remains required

The generic protocol treats application bytes as opaque. A caller must validate lifecycle before preparing an append; a fresh caller can otherwise replace that descriptor. These controls prove original-head fencing of previously prepared operations, not complete client Signal/purge atomicity. No current runtime path opts into v4. The current graph journal cursor deliberately requires v3 and needs a versioned runtime migration before new forests can share its destination. Fixed forest/reader limits and the256KiB root bound fail closed; multi-forest reader capacity at larger frontiers remains unqualified.

Canonical incoming Start/Signal publication requires durable pending-operation recovery, exact invocation identity, graph-owned staging, ordered signal binding, scalable idempotency lookup, reader/writer/reconciler/purge/discovery integration and a quiesced namespace/schema deployment. Preserving the original WF_INV/WF_SIG sequence and queue-order contracts requires explicit source-write reservation and crash/reply-loss qualification; a compatibility mirror or index alone cannot supply this proof.

All remaining canonical state/timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment paths, complete current142 simulation and all original native concurrency/capacity/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production online collection remains disabled; the full goal stays active.
