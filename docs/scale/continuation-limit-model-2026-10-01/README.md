# Seeded global continuation entry limits

Base main: 8a5e25e9818422c6d031c21c298c48eb5267cb57. The production
JetStream-facing Worker constructor and append guards are unchanged. The
modeled constructor accepts JournalEntryLimit to lower its global budget for
bounded boundary schedules: zero retains 100,000, only 4..100,000 is accepted.
The journal's hard cap remains 100,000. This is not a lower production default
or a replacement for the independently proved real 100,000-entry boundary.

## Workload and assertions

Production workers publish two checkpoint frames and suspend on a gate.
Budgets 16/18/20 move padding through the initial/middle stages. Pre-resume
logical count is budget-3, frame anchor budget-7 and SDK offset budget-8.
Replacement uses a frame reader that rejects archived-prefix reads. Seven
modes cover clean execution, gate SignalConsumed lost before commit/acknowledgment
lost after commit, gate StepCompleted lost/hidden acknowledgment, and reserved
Failed terminal lost/hidden acknowledgment. Every one of the 21 combinations
is exercised.

The enabling signal and its completion fit, the next effect request is rejected
before callback execution, exactly budget logical entries remain, Failed occupies
budget-1, and retained LimitRequest binds the rejected declaration. Outcome KV
bytes equal the immutable terminal. Prefix handlers are not re-entered during
replacement. The full logical history passes raw integrity. Explicit CLI-style
rejected-request substitution replays both frames to ErrReplayPendingStep at
budget-5 SDK entries without effects; a changed declaration is nondeterministic.

## Results

- 100,000 seeds PASS in 128.607 seconds: 200,000 scheduler choices,
  21,025,331 transport events, maximum virtual time 1,000 ms.
- Focused workload, constructor bounds and full pinned corpus under race PASS
  in 27.167 seconds; workload itself 24.40 seconds. First ten seeds replay
  exactly; seed 42 produces identical bytes in two separate processes and is
  pinned as continuation-limit-42.json.
- Constructor tests accept 0/default, 4, 16 and 100,000 and reject 3/100,001.
- Matching existing real R3 budget-16 race contract PASS in 18.305 seconds:
  16 logical entries, frame anchor 9, SDK offset 8, zero effects/archive reads,
  11 offline SDK entries and two continuations.
- Compiled production guard mutation uses only retained suffix length for
  budget checks while leaving absolute append identities intact. Semantic FAIL
  in 0.006 seconds: 22 logical records under budget 20. No build errors or
  unrelated timeout count as this control.
- Vet and diff whitespace checks PASS.

```sh
SIM_SEEDS=100000 SIM_COVERAGE_SUMMARY=1 go test ./sim -run '^TestSeededContinuationLimitReplay$' -count=1 -timeout=10m -v
go test -race ./sim -run '^Test(SeededContinuationLimitReplay|ModeledJournalEntryLimitBounds|PinnedRegressionCorpus)$' -count=1 -v
go test -race ./worker -run '^TestContinuationPreservesGlobalJournalLimitAndTerminalSlot$' -count=1 -v
go vet ./worker ./sim
```

The actual production-cap continuation proof remains in
../continuation-production-limit-2026-10-01 and hosted run 36810716129. Small
modeled budgets exercise the same append decisions, not 100,000 records in each
seed. Matching real transport contracts for these combined near-limit reply
cuts, server/process faults near the cap, seeded GC/retirement and full final
release gates remain open. CLI-style substitution is explicit; Replay does not
automatically interpret Failed.LimitRequest. No existing pin was regenerated.
