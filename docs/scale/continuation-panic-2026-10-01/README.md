# Continuation panic budget across checkpoints and restart — October 1, 2026

A real R3 three-node contract uses a maximum of three journaled handler panics.
The initial handler panics once, then stores state 10 and continues to middle_v1.
That stage reads the restored state, panics once, then stores state 20 and
continues to finish_v1. The final stage suspends awaiting a signal. The first
worker is stopped, and its frame is decoded to verify stage finish_v1 and a
panic-attempt baseline of two.

A replacement worker pinned to another peer uses an archive-denying journal
port. After the enabling signal it restores state/locals 20 and panics with
final poison. The invocation fails immediately on its third total panic:
initial and middle handlers each ran twice, the final handler ran exactly once
more after replacement, and the logical journal has Attempt counts 1, 2, 3
followed by Failed. All three peers return the identical failure and the raw
integrity checker finds one invocation and one terminal. Archive reads are zero;
the replacement reads a verified frame.

## Evidence

- `real-race.log`: PASS, 20.925 seconds total, 19.89 seconds test time.
- `budget-mutation.log`: a compiled Go overlay replaces the production worker's
  `attempts := int(checkpointInfo.PanicAttempts)` with `attempts := 0`.
  The contract fails before the final stage with invalid step protocol in the
  journal in 5.708 seconds total. This is a behavioral failure, not a compile
  failure. The worktree's production code was never modified.
- `vet.log`: go vet ./integration exited zero without diagnostics.
- `SOURCE_SHA256SUMS`: tested source and runtime dependency hashes.

This proves normal panic retry accounting through two boundaries and graceful
worker replacement. It does not prove SIGKILL between Attempt and Failed,
reply-loss combinations involving those records, a seeded integrated panic
workload, or every continuation SDK acceptance gate. Existing publication SIGKILL
proofs are separate. Final-source full matrix, 24-hour soak, online GC and the
mixed seed 65 latency failure remain open. Runtime source is unchanged from
parent effe9c8; this change adds a test and evidence only.
