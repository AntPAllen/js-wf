# Foreign fallback timer hints

Graph fallback recovery scanned the shared `WF_TIMER` stream but treated a quorum-confirmed absent local Start reservation as uncertainty. A hint owned by a different graph store could stop the pass before it reached this store's due timer. Native timer and suspended history scans already receive empty history for confirmed absent roots.

`before-fix.log` reproduces the fallback omission using in-memory graph/source transports at seed 17 with no NATS server: namespace B owns the first hint; A owns the second. A stops at the first with `ErrUnknown`, inspects one record and publishes nothing. The new fixture was added to the f18a978 production source before changing the decision. This is a directed model reproduction, not a registered replay/minimization or 1,000-seed family acceptance.

The corrected decision skips only confirmed absent local reservations. It neither wakes nor deletes that shared hint, and confirms cursor progress so later owned hints can be repaired. A pending local reservation or unknown authority observation still stops the scan. No legacy state, input or ownership fallback is introduced. Later catalog passes can revisit skipped hints.

## Evidence

`race.log` passes the final exact command in `results.json` with actual child exit zero and package time 2.940 seconds. The directed fixture verifies two independent graph models sharing invocation/timer source ports: A wakes/deletes only its own second hint; B subsequently wakes/deletes its own first hint. Wakeup identity bytes and partition subjects are checked. Pending-local and dropped authority-read controls publish/delete nothing and certify no cursor prefix. Existing nine fallback decision modes and the native R1 forged-projection/purge-state fixture also pass.

`development-build.log` retains an initial test subject-helper signature error. `development-counter.log` retains an incorrect fixture assertion expecting due-hint deletion to increment `Removed`; that field counts retirement cleanup, while successful due wakeups use `Reenqueued`. The corrected fixture checks actual deleted hint sequence numbers. `initial-race-pass.log` passed before explicit wakeup identity-byte checks were added; the final run includes them. No runtime counter semantics or deadlines changed.

## Remaining scope

This fixes one shared-source fallback decision. It does not establish namespace-aware dispatch ownership or complete multiple-store worker rollout, nor qualify new R3 route/storage/process/VM/clock/scale faults. Registered inventory remains 155 families/835 traces. A shared seeded fallback namespace workload and complete current normal/race/all-pin/extended qualification remain open. Frozen live/queued campaigns exclude this later fix. Production collection stays off and all original broader runtime/lifecycle/purge/retention/migration/scale/soak/drain/release requirements remain open.
