# Canceled continuation timer and deterministic version replay — October 1, 2026

## Runtime bug and fix

A native scheduled wakeup for a timer canceled before a continuation checkpoint
was recognized for metrics but still dispatched the active stage. The real R3
reproduction failed with stage entries 2 instead of 1 in 15.487 seconds. A seeded
production-worker reproduction failed on seed 1 in 0.005 seconds with the same
extra entry and unchanged 14-record history. Journal immutability alone would
miss this bug. The existing terminal-workflow cancellation tests did not expose
active handler reentry.

The worker now acknowledges confirmed canceled timer wakeups before user handler
dispatch. Generation, checkpoint, input and terminal checks remain ahead of that
return. In particular, a terminal journal still repairs its outcome publication
without rerunning a handler.

## Real contract

The initial handler records Version=2, creates a 12-second native timer at
absolute SDK step 2, cancels it, checks ErrTimerCancelled and checkpoints
finish_v1. The frame at SDK position 8 retains the cancellation; the live suffix
has no cancellation declaration. The stage records another Version=2 and waits
for a gate. The first worker stops before the real native deadline.

A replacement pinned to another peer rejects archive reads. With the compatible
version maximum changed to 3, the actual canceled native delivery must not enter
either handler or change the logical history. The gate then enters the stage
once and replays Version=2, returning 2. All peers agree, raw integrity finds one
invocation/terminal and full offline staged replay verifies all 12 SDK entries.
The real contract plus existing terminal timer cancellation passed under race
in 23.296 seconds (16.54 and 5.72 seconds individually).

## Seeded contract

The production client, SDK, leased worker and journal use modeled native timers,
consumer dispatch, snapshot objects and KV. Seven modes cover clean execution,
timer publish drop/hidden ACK, cancellation append drop/hidden ACK, wakeup ACK
loss and consumer leader change. Two deadlines (5 and 7 virtual seconds) produce
14 required combinations. Every injected fault must be consumed. No early
wakeups, repeated initial/stage execution on cancellation, journal drift,
archive access or changed Version decisions are permitted.

A separate boundary removes terminal state from the fixture while preserving
the terminal journal. Redelivering the canceled wakeup must restore exactly the
saved outcome without handler entry or journal changes. This constructs the
reachable journal-before-state cut; it does not model loss of acknowledged KV
data. This repair boundary is modeled evidence, not an actual process-kill test.

100,000 schedules passed in 140.654 seconds with 200,000 choices, 23,960,419
transport events and maximum virtual time 7,100 ms. First-ten exact replay,
byte-identical seed-42 traces from separate processes and all 14 combinations
are asserted. Seed 42 is pinned in the shared regression corpus. The workload
and full corpus passed under race in 23.797 seconds; worker race passed in
32.414 seconds. The full simulator suite passed in 116.150 seconds. Vet exited
zero without diagnostics.

## Behavioral negative controls

- `original-mutation.log` / `original.patch`: restoring the original worker
  makes the new pin reenter the stage (2 instead of 1), failing in 0.009 seconds.
- `terminal-order-mutation.log` / `terminal-order.patch`: moving the early return
  before terminal repair leaves the outcome missing, failing in 0.013 seconds.

Both controls compile and fail the selected semantic assertion. They use Go
overlays and do not mutate the worktree. `real-before.log` and `model-before.log`
retain the original behavioral failures. `SOURCE_SHA256SUMS` identifies final
sources and shared helpers. Other logs retain final validation.

This closes the focused canceled-wakeup/version replay slice. Combined faults,
actual canceled-timer terminal-repair SIGKILL, full 100,000-entry continuation
limits, other continuation acceptance cuts and final-source release gates remain
open. The mixed seed 65 latency miss and online GC remain open. Existing long
running timer and extended simulation campaigns have not been restarted.
