import json,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-streams-2026-10-08'
review=json.loads((base/'review.json').read_text());runs=json.loads((base/'results.json').read_text())['runs'];assert review['all_six_commands_pass'] and len(runs)==6
rows='\n'.join('| '+r['name']+' | '+str(r['package_passes'][0]['elapsed'])+' | '+str(len(r['top_level_passes']))+' |' for r in runs)
(base/'README.md').write_text(f'''# Independently indexed canonical graph forests — 2026-10-08

Frozen source `{review['source']}` passes all six verification commands. Every one of the {review['tracked_inputs']:,} selected tracked Go/YAML/module/pin inputs matches frozen Git before, after and at independent review. Every command uses count1, two Go CPUs and512MiB. The100,000-schedule normal simulation's ten-minute bound was selected before launch from the measured fixture cost; other commands retain five-minute bounds. No failed gate was relaxed or rerun.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
{rows}

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
''')
shutil.copyfile(__file__,base/'executed-documentation.py')
summary='''

### 2026-10-08 — independently indexed graph forests verified

Frozen `647c933` passes complete graphpublication normal/race (58 groups, including six new deterministic groups and native R1/R3 large payload/adapter reopening/reader retention/physical drain),14 graph journal groups,100,000 new-family normal/1,000 race schedules with exact replay and actual body accounting, and every670 pin in both commands. All654 prior pins remain byte-for-byte unchanged;16 new pins cover all16 modes. Independent source-before/after/current/Git, compiled142-family inventory, native events, mode counts and CI family review are retained. [Reviewed evidence and exact timings](scale/graph-streams-2026-10-08/).

A versioned v4 authority root now supports independently indexed append forests under one shared CAS head. Intent locations include their forest name; commit preserves every other forest. Reader pins/checkpoints retain the complete captured set, and whole retirement clears live forests together while preserving readers and lifecycle/high-water metadata. Cross-forest edge reuse requires copying bytes into a fresh grant. Generic application metadata remains opaque; this is the storage primitive for incoming publication, not runtime Signal/purge adoption.

Continue canonical incoming Start/Signal publication with durable pending-operation recovery, preserved invocation/signal sequence and queue-order contracts, graph-owned staging, scalable idempotency lookup, v4 runtime cursor/readers/writers/reconcilers/purge/discovery and quiesced namespace/schema rollout. The current runtime still requires v3 and does not opt into v4. Complete current142 simulation, native capacity/concurrency/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release and production online collection remain open. No gate, old failure or full-goal scope is reduced.
'''
p=repo/'docs/implementation-status.md';p.write_text(p.read_text()+summary)
p=repo/'docs/implementation-plan.md';p.write_text(p.read_text()+summary.replace('### 2026-10-08 — independently indexed graph forests verified','## Independently indexed canonical graph forests verified — 2026-10-08'))
p=repo/'docs/retained-append-graph.md';p.write_text(p.read_text()+'''

## Independently indexed forests under one authority

The v4 graphpublication root now holds up to four named append forests beside the original unnamed journal forest. Each has its own record index; all publication, reader, application and retirement operations still share one destination/head CAS. Names qualify exact node/payload intent locations, commit preserves all unselected forests and the collector checks the correct live/pinned forest. Readers capture the complete set and checkpoint it with a versioned fingerprint. Same-forest reuse preserves origin grants; cross-forest transfers currently copy bytes into a fresh grant. All live forests retire together, with old pins remaining protected.

[Complete normal/race/native/100,000-seed evidence](scale/graph-streams-2026-10-08/) qualifies this generic storage primitive. The current graph journal deliberately requires v3; no runtime client/worker opts into v4. Application lifecycle remains caller-validated opaque metadata. Independent indexes do not by themselves provide canonical Start/Signal publication, ordered signal binding or idempotency lookup. A fresh caller must reject fenced lifecycle before preparing; shared-head CAS invalidates previously prepared operations.

### Incoming runtime integration requirements

- Preserve start-once input/parent identity and the original invocation generation contract. A canonical pending start must own its input before any source write; unknown/crashed source publication must be recoverable from its exact durable identity. Begin/dispatch may not infer a ready generation from a compatibility message alone.
- Preserve the original WF_SIG sequence order and per-invocation linearizable queue contract. Canonical signal publication needs a durable reservation and exact source-sequence binding, including crashed/lost-reply writers and competing signallers. Merely appending graph records in whichever CAS order succeeds cannot establish the existing source-order contract.
- Persist pending-operation ownership and scalable key lookup. Permanent idempotency may not be implemented as an ever-growing root map or unbounded history scan. Pending input/signal bytes must belong to exact graph leaves; application JSON pointers cannot grant ownership. Repair must settle an unknown outcome before replacing its logical operation.
- Migrate the runtime cursor, journal append conflict rules, graph views, input/signal/child/cancel clients and workers, all discovery/repair and purge paths together. A concurrent incoming append must not change the logical runtime journal index or permit a stale worker epoch to refresh its fence.
- Isolate/version the source namespace and quiesce incompatible actors before rollout. Old strict adapters must fail closed; unconfigured legacy workers cannot be allowed to execute pending pointer envelopes. Late compatibility writes after retirement may not authorize execution or deletion of a replacement generation.
- Qualify seeded crash boundaries and replay first, then original real concurrent signallers, linearizable Start/Signal histories, native partitions/server/process/storage/power loss, reader capacity/scale, full matrices/24h/million physical drain and deployment/default-adoption/release. This primitive does not discharge those gates or enable production collection.
''')
