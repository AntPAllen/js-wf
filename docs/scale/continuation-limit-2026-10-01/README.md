# Continuation global journal budget and terminal reservation — October 1, 2026

The new worker-package contract uses a real R3 three-node fixture and two SDK
checkpoints. As in existing worker limit tests, it lowers the private worker
budget to 16 for both worker instances. The production default and hard journal
cap remain 100,000. No production limit option or runtime change is introduced.

The initial handler sets state 10 and continues to middle_v1. That stage reads
state/locals 10 and checkpoints finish_v1. The final stage suspends on a gate.
The first worker stops with 13 logical records, last index 12. Its final frame
anchors logical index 9 and absolute SDK position 8. Prefix handlers may replay
before manifest publication; their counts are sampled after first-worker stop.

## Resume and reservation

A replacement pinned to another peer uses an archive-denying port and the same
16-entry budget. The enabling signal consumes index 13 and completes its SDK
wait at index 14. A requested effect at index 15 would lack completion and
terminal capacity. The worker must record Failed at index 15 with the exact
rejected request in LimitRequest, without invoking that effect.

The final full logical history has exactly 16 entries. The initial/middle entry
counts do not increase during replacement. One verified frame is read with no
archive access. The effect count is zero. Terminal state matches Failed byte
for byte, all peers return journal.ErrTooLong, and raw integrity finds one
invocation and one terminal.

## Offline boundary audit

The test follows the CLI's explicit limit-audit protocol: replace Failed with
its retained rejected StepRequested and replay the full staged history using
both checkpoint objects. Replay must consume the same declaration and stop
with ErrReplayPendingStep before the effect, after two continuations and all
11 recorded SDK entries. A changed rejected declaration must be nondeterministic.
This is an explicit boundary audit; ReplayWithContinuations does not automatically
interpret the original Failed.LimitRequest metadata.

## Evidence and limits

- `real-race.log`: final R3 contract passed in 17.983 s (16.94 s test time).
- `suffix-budget-mutation.log`: compiled production overlay replaces absolute
  nextIndex checks with suffix-only len(records) checks for both the hard budget
  and reservation predicates. The workflow completes instead of failing, so the
  contract rejects it in 16.850 s. The exact edit is retained as a patch. This is
  a behavioral failure, not a compile failure, and does not alter the worktree.
- `vet.log`: worker vet exited zero without diagnostics.
- `SOURCE_SHA256SUMS`: final test and runtime dependency hashes.

This proves the global accounting/reserved-effect-request path at a small test
budget. It does not prove a full 100,000-entry continuation workload, limit-adjacent
SIGKILL/server faults or every non-SDK reservation boundary. Existing hard-cap
and ordinary limit contracts are separate. Seeded integrated continuation limits,
remaining timer/cancellation/version gates, full matrix/24-hour soak, mixed seed
65 latency and online GC stay open.

The retained CI run 36806383537 at 8f6789a exhausted the aggregate 20-minute
integration timeout while a visibility test had run for only six seconds.
`ci-8f6-suite-timeout.log` preserves the full timeout stack. Commit 0821f8c already
increases that aggregate suite budget to 30 minutes (35-minute job); fresh CI
validation is pending and this does not change individual liveness targets.
The broad 100,000-seed Tier 1 campaign at 0821f8c and original million-timer
runner remain active without restart.
