# Resolved continuation promises, worker replacement and retirement — October 1, 2026

A real R3 three-node worker executes a parent with one CallAsync child. The child
returns a 614,402-byte JSON result, spilling into a content-addressed terminal
blob. The parent consumes its outcome once, resolves the blob and continues to
finish_v1 with explicit wf.Promise locals. The frame is only 579 bytes: it holds
the serialized outcome reference/hash and consumption facts, not the derived
child result cache. The final stage suspends awaiting a gate signal.

All worker loops stop before every quiescent sweep and retirement operation.
Attempting child retirement before its parent is terminal must return
ErrNotTerminal; the existing parent-retention rule is preserved. An initial
sweep preserves all live objects.

## Replacement and offline replay

A replacement pinned to another peer reads the frame through an archive-denying
journal port. After the enabling gate signal, it awaits the saved promise twice.
The first returned slice is deliberately changed; the second must still equal
the original child bytes. The production result-blob transport is wrapped with
the new WithResultBlobPort option to count loads separately from journal/frame
reads. One verified child read serves both awaits. The parent returns 614402.

The initial handler runs twice before its checkpoint (initial suspension and
child-result resume); it never runs again during replacement. The child runs
once. Full logical history contains one call_async request, one child
SignalConsumed and one child await request; repeated restored awaits add no
SDK steps or additional signal consumption. Every peer returns the same parent
result, and full staged offline replay consumes all eight SDK entries and
returns the same value without invoking the child again.

## Retirement and collection

After parent completion the child can retire. A quiescent sweep must preserve
its exact result bytes through the retained parent's frame-held promise. The
raw audit then sees one current invocation and one terminal (the parent).
Retiring the parent permits the next sweep to reclaim exactly three objects:
child result, parent frame and parent archive. Every object is absent afterwards.

The compiled negative control skips marking frame.PromiseOutcomes in the real
collector. The test then fails because the child's blob is deleted after child
retirement. This proves the lifecycle assertion depends on the frame-held
promise reference, even though the parent's archive is also retained.

## Evidence and scope

- `real-race.log`: PASS in 18.250 seconds (17.21 seconds test time), one child,
  one replacement frame read, zero archive reads, one child blob read, one
  consumption, three objects collected only after parent retirement.
- `promise-reference-mutation.log`: compiled collector overlay fails with
  object not found after child retirement in 17.070 seconds. The exact edit is
  retained in promise-reference-skip.patch. This is not a compile failure and
  never modifies the worktree's collector source.
- `worker-sdk-race.log`: worker race suite passed in 14.433 seconds; SDK race
  suite passed using its unchanged cached result.
- `vet.log`: integration/worker vet exited zero without diagnostics.
- `SOURCE_SHA256SUMS`: tested fixture, shared guard and runtime dependencies.

The production change is an optional result-object transport injection;
invocation, journal, leasing and outcome decisions retain their existing paths.
This is a focused normal restart/promise-retirement proof, not a promise
SIGKILL/fault-combination matrix or seeded integrated promise workload. Broader
continuation timer/cancellation/limits, final-source matrix and 24-hour soak,
online GC, mixed seed 65 latency and remaining capacity campaigns stay open.
The million-timer campaign continues without restart.
