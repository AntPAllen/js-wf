# Continuation panic terminal SIGKILL cuts — October 1, 2026

A Linux test starts a real worker subprocess pinned to one peer of a three-node
R3 JetStream fixture. It panics once in its initial handler, stores state 10 and
continues to middle_v1. That stage reads state/locals 10, panics once, then stores
20 and checkpoints finish_v1. The final stage reads state/locals 20 and panics
with final poison. The invocation's maximum panic count is three.

A test-only operation observer holds the execution goroutine at an acknowledged
journal append. Heartbeats can continue until the parent kills the process.
The parent independently reads the real retained journal and runtime frame:

1. **after_attempt:** Attempt counts 1, 2, 3 are durable; the tail is Attempt,
   no Failed exists, and WF_STATE has no terminal outcome.
2. **after_failed:** all three Attempts and Failed are durable; WF_STATE still
   has no terminal outcome because the worker is held before outcome publication.

At both cuts the verified runtime frame names finish_v1 with PanicAttempts=2.
The synced handler log records exactly two initial calls, two middle calls and
one final call. The parent sends Process.Kill and verifies SIGKILL via WaitStatus.

## Recovery contract

A replacement worker pinned to another peer has poison handlers that record
any unexpected entry and panic. Its journal reader rejects archive-object reads.
Recovery relies on the original unacknowledged run; the test publishes no repair
or synthetic wakeup. Once its prior lease expires, the replacement must:

- Return exactly workflow panic: final poison and publish matching terminal state.
- Enter no handler: the synced log remains byte-for-byte unchanged.
- Read a verified runtime frame with zero archive fetches.
- Preserve every pre-kill logical journal record byte-for-byte.
- At after_attempt, append only Failed under a higher fencing epoch.
- At after_failed, leave the entire journal unchanged and materialize its outcome.
- Return the same failure from every peer and pass the raw retained-state audit
  with one invocation and one terminal.

Each sample requires recovery below 30 seconds. These are individual samples,
not a release p99 distribution.

## Evidence and limits

The final race run passed in 40.714 seconds. Recovery measured 13.015030476 s
at after_attempt and 13.024369137 s at after_failed. Both histories have 19
entries. At after_attempt the epoch advanced from 34 to 40; after_failed kept
its existing terminal epoch 34. Both replacements read one frame and no archive.

`exhaustion-mutation.log` records a compiled production overlay that replaces
`if attempts >= w.maxPanicAttempts` with `if false`. The after_attempt contract
fails in 19.826 seconds because replacement reruns its poison handler and returns
workflow panic: budget recovery reran handler. This is a behavioral failure,
not a compile failure; the worktree's runtime is never modified. The exact edit
is retained in exhaustion-bypass.patch. Vet exited zero without diagnostics.
Source hashes bind the tested fixture, shared helpers and runtime dependencies. Production runtime
source is unchanged from parent 3a5ed7e; only test and evidence are added.

The existing continuation_panic Tier 1 workload covers individual hidden ACK/
dropped Attempt and Failed writes; this fixture adds actual process death.
It does not establish a seeded process-kill schedule, combined faults, leader/
route faults at these cuts or every continuation SDK acceptance gate. The
final-source matrix and 24-hour soak, mixed seed 65 latency failure, online GC
and remaining capacity campaigns stay open. The original million-timer runner
continues without restart.
