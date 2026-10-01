# Offline audit of canceled continuation histories

Base: 043e7e78d6a22cdaec2d4311f51fbde8470a18dc. The source hash file
identifies final runtime/test files and the unchanged seed-42 trace.

## Runtime defect and fix

The first focused contract found that ReplayWithContinuations accepted a Failed
terminal payload from invocation generation 18 while ReplayOptions required 17,
then entered all three replay handlers and returned ErrReplayPendingStep. The
Failed path never bound terminal identity, unlike Completed return validation.

Replay now checks a Completed/Failed tail's payload and invocation identity
before user code whenever the caller supplies identity. Failed must also carry
an error. This applies to ordinary Replay and staged replay through the shared
implementation. Histories replayed without identity retain legacy behavior.
No worker, lease, dispatch or journal writing behavior changes.

A canceled effect still has no recorded completion. Offline replay must verify
its pending request, return ErrReplayPendingStep and never invoke its callback;
it must not manufacture an effect outcome or replace that replay stop with the
terminal cancellation. The durable Failed payload is the actual workflow
outcome. Changed pending declarations remain nondeterministic.

## Verification

- Complete wf package race suite PASS, 7.851 seconds. The new two-checkpoint
  fixture stops at the named run_once suffix request, then records cancellation
  and Failed. All 15 SDK entries replay, with zero callback execution. Changing
  the pending declaration fails; wrong-generation, missing-error and malformed
  terminal payloads fail before any replay handler.
- Compiled old-runtime overlay: semantic FAIL in 0.004 seconds, wrong-generation
  payload 18 accepted with ErrReplayPendingStep and three handler calls.
- Compiled callback-execution mutant: semantic FAIL in 0.003 seconds, returning
  ErrReplayPendingStep but executing the pending effect once (effects=1).
- Expanded seeded active-continuation cancellation: 100,000 schedules PASS in
  107.520 seconds, 16,988,200 events, maximum virtual time 15 seconds. Each
  generated terminal history is also audited offline through all nine SDK
  entries, with no callback. Seven client/notification fault modes remain covered.
- Focused seeded and complete pinned corpus race suite PASS, 15.663 seconds.
  No pinned trace was regenerated: offline checks issue no transport operations
  and exact replay retains the original bytes.
- Full simulator package PASS, 115.103 seconds, at the default seed count.
- Real R3 notification and missed-notification/durable-poll contracts PASS
  under race, 49.834 seconds. Both confirmed cancellation histories reconstruct
  offline through nine SDK entries. Individual cancellation times were
  117.577 ms and 14.723 seconds; these are not p99.
- Vet and diff whitespace checks PASS.

```sh
go test -race ./wf -count=1
go test -race ./sim -run '^Test(SeededContinuationRunningCancelReplay|PinnedRegressionCorpus)$' -count=1 -v
SIM_SEEDS=100000 SIM_COVERAGE_SUMMARY=1 go test ./sim -run '^TestSeededContinuationRunningCancelReplay$' -count=1 -timeout=10m -v
go test ./sim -count=1 -timeout=10m
go test -race ./integration -run '^TestContinuationCancellationInterruptsActiveSuffix$' -count=1 -v
go vet ./wf ./sim ./integration
```

The controls initially revealed that a suffix-length cut stopped at a later
state read rather than the effect. The final unit fixture locates the effect
request by kind/name, and the callback mutant confirms that exact boundary.
Only the final semantic failures above count as controls.

This closes the focused offline canceled-pending-effect audit left open in the
previous slice. Combined publication/GC/cancellation cuts, the full final-source
Tier 1 campaign, 200-seed real matrix, five-node 24-hour soak, mixed latency
causes and online GC remain open. Original long-running jobs were not restarted.
