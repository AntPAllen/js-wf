# Tier 1 continuation panic budget — October 1, 2026

The new continuation_panic workload runs production client, leased worker,
journal, snapshot publication/read and SDK continuation decisions against the
seeded in-memory transport. It is included in the standard sim suite and the
extended 100,000-seed CI suite; saved traces use the shared replay dispatcher.

Each schedule chooses one or two prefix panics, one or two middle-stage panics,
and a budget of their sum plus one (three to five). Two checkpoints preserve
state 10 then 20. The final stage suspends on a signal. A fresh worker restores
finish_v1 with the exact saved panic count, using a reader that rejects archive
objects. The enabling signal makes the final handler panic. It must fail on
the same invocation-wide limit with contiguous Attempt counts and final poison.
The raw retained-state audit requires one invocation and one terminal.

## Faults and observations

Nine modes cover clean execution, drop-before-commit and hidden committed ACK
for each of final Attempt, final Failed, checkpoint frame and archived prefix.
All 36 combinations of mode and prefix/middle panic counts must be observed.
Journal faults are armed only after the final stage's checkpoint/suspension.
Object faults occur during the first checkpoint. Every injected fault must be
consumed; no schedule may pass because its fault did not happen.

The replacement must not enter initial/middle handlers or fetch an archive.
Dropping the final Attempt causes exactly one extra final handler call; the
unrecorded panic did not consume durable budget. Lost Attempt ACKs resolve the
committed record. Dropped/hidden Failed writes recover from the final Attempt
without rerunning the handler. The exact outcome, counts, state, signal
selection and generation are verified through reconstructed history and audit.

Exact replay of the first ten schedules and byte-identical seed-42 traces from
two separate processes are required. Seed 42 is pinned in the regression corpus.
A compiled overlay resetting the saved budget fails the pinned trace with an
invalid step protocol before final suspension. It is a behavioral failure,
not a compilation failure, and never modifies production source in the worktree.

## Evidence and scope

- `100k.log`: 100,000 clean schedules, 300,000 scheduler choices and
  29,688,450 transport events in 183.074 seconds; maximum virtual time 16 seconds.
- `race-corpus.log`: new workload and complete pinned regression corpus passed
  under race in 41.690 seconds, including exact replay of the new pin.
- `suite.log`: complete simulator suite passed in 120.702 seconds.
- `budget-mutation.log`: compiled production overlay fails seed 42 in 0.008
  seconds with a behavioral protocol error. `budget-reset.patch` records the edit.
- `vet.log`: vet exited zero without diagnostics.
- `SOURCE_SHA256SUMS`: tested sources, shared model helpers and pinned trace.

The seed loop verifies all 36 combinations. The first ten traces replay exactly;
two child processes generate identical seed-42 trace bytes.
Production runtime source is unchanged from parent fbe427a.

The matching real R3 restart contract is in the separate continuation-panic proof.
This model does not simulate Raft, process death, disk persistence or the exact
server replies behind a real-cluster delay. It selects one fault per schedule;
combined faults, Attempt-to-Failed SIGKILL and broader continuation semantics
remain open. It is not full final-source Tier 1 release evidence, a clean Tier 2
matrix or the Tier 3 24-hour soak. The mixed seed 65 latency miss and online GC
remain open; the original million-timer campaign continues without restart.
