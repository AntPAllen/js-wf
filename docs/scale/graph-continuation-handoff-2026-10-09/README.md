# Canonical graph continuation handoff boundary — 2026-10-09

`GraphStore.ConfirmCheckpoint` acquires a fresh pin, validates the exact owned
checkpoint/runtime pointer and observed tail, and releases the pin before
reporting confirmation. Missing/changed checkpoints and stale tails fail.
Uncertain reader release cannot authorize handoff.

The production worker continuation publisher now has a graph branch: lease
renewal, owned checkpoint confirmation, suspension append and final lease
renewal precede a fresh enqueue. Retrying an already-suspended checkpoint avoids
a second suspension but publishes another recovery dispatch inside the dedup
window. The legacy branch retains archive/manifest/purge publication and its
stable message ID.

**Continuation admission remains closed.** This branch is exercised directly
as a migration component, not enabled for users. It does not compact graph
history or establish stage dispatch/recovery. Full continuation execution,
archive/prefix compaction, materialized payload ownership through collection,
resume discovery, audit/offline replay and native kill/limit qualification remain
required before admission. Atomic cross-stream lifecycle fencing remains open.

## Executed development evidence

- `component-race.log`: twenty JSON/protobuf checkpoint cases and historical
  continuation anchor controls pass. Valid cases additionally require successful
  exact confirmation, stale-tail rejection, pointer mismatch rejection, and a
  failed confirmation when the reader-release CAS is uncertain.
- `final-handoff-race.log`: direct native R1 production-publisher control passes.
  Invalid checkpoint metadata publishes nothing; valid publication creates one
  suspension and one dispatch; retry produces a second dispatch without another
  suspension; a released lease prevents further publication. No WF_JRN writes
  occur. This calls the publisher directly; no continuation handler runs.
- `legacy-limit-race.log`: existing legacy continuation global-limit/terminal-slot
  test passes under the race detector after the publisher branch change.

The graph/source model remains seeded and in memory; the native handoff test is
R1. Tests ran in the development checkout; source hashes are observed at review,
not a frozen qualification snapshot. These controls establish no full/extended
suite, R3 handoff, process-kill, compaction, scale, actual24h, physical-drain,
default-adoption or release gate. The live frozen race at `9a1ccdc` excludes these
changes, and every original remaining broader requirement remains open.
