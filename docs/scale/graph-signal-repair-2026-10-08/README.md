# Automatic canonical Signal repair

The opt-in `graph-signal` repair loop discovers canonical authority roots, repairs reserved publications using retained request/token/input ownership, and reenqueues bound unconsumed Signals using metadata only. Each root pass examines at most eight reservations and persists its position. Bound wakeup repair does not acquire reader pins or change the runtime head. Successful publication recovery already enqueues a wakeup; the same pass does not enqueue it again.

Canonical Signal cursor v4 adds repair position and a validated consumed prefix. Each SignalConsumed journal publication must prove the exact next queue index, token, source sequence, name, input hash, canonical reference and retained body ownership; the prefix advances in that same CAS. Prior experimental v3 roots are rejected rather than assuming their historical consumption was zero. Migration/import for those roots remains open. Default journal and canonical Start modes keep their existing schemas.

Development evidence:

- `development/scanner-final.jsonl`: seeded bounded restart, dry run, unknown outcome, prepared append interleaving and retirement fencing controls pass.
- `development/consumption-corrected.jsonl`: forged metadata, absent ownership, unknown metadata read and successful consumption controls pass.
- `development/native-ready.jsonl`: R1/R3 actual repair loops recover reserved, source-committed and bound/source-purged cuts after reopening storage. All three workflows per replica configuration consume the correct input, complete with 42 and execute three effects total.
- `development/normal.jsonl`: journal fixture failures preserved; client, reconcile and canonical worker cases passed. Consumption fixture incorrectly omitted Started and named an unsupported read fault; corrected evidence is separate.
- `development/native.jsonl`: initial incorrect Await signature build failure preserved.
- `development/native-corrected.jsonl`: purging an already-enqueued wakeup left its native dedup identity remembered; the bound fixture timed out. This was not a bound-before-enqueue cut.
- `development/native-no-enqueue.jsonl`: corrected R1 passed; R3 provisioning timed out before cluster readiness. The existing fixture readiness wait was then added; native-ready passed.

Frozen `f5fa502` passes all six qualification commands: Signal components, the full client package and all 710 pinned replays in both normal and race modes. The component commands cover 11 journal, five reconciliation and two canonical worker groups; the full client commands cover 12 groups. Separate executed review verifies all 1,675 selected committed Go/config/corpus inputs unchanged before/after, exact package/group outcomes, the 112 publication fault cuts per mode, every replay pin, native R1/R3 recovery and all eight child-transfer cases per mode. Commands, source hashes, JSON event streams and review are in `qualification/`. No test skips or data races were reported.

The earlier `development/race.jsonl` retains the same initial dedup/provisioning fixture failures; its journal/client/model passes are development evidence, not frozen qualification. These component fixtures do not prove VM/process kill recovery, the complete shared simulation flow/family corpus, the original extended campaigns, network/storage faults, scale, a 24-hour soak, million-object drain, default adoption or production online GC. Those gates and runtime state/timer/snapshot/continuation/import/discovery/deployment migration remain open.
