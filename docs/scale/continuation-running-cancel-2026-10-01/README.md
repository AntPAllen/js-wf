# Cancellation of an active checkpoint continuation

Base main: 9d2212b2fd0d9f3467e5cd82b2045dd04ac1bdfc. No production runtime
changes. Source hashes identify the tested fixture, model and shared helpers.

## Contract

The initial handler sets state 23 and publishes finish_v1 with locals 45. It
suspends on a gate after checkpoint handoff. A replacement resumes from the
frame with archive reads forbidden, verifies state/locals, consumes the gate
and enters a blocking journaled effect. A stale-generation cancel must leave
its context active. A matching cancel must interrupt that context and append
exactly one cancel consumption plus Failed{cancelled}, with no effect completion.
The confirmed logical prefix remains byte-identical and the prefix handler is
not entered after replacement. Final raw integrity and immutable outcomes pass.

The seed model runs the production client, lease, journal, frame and worker
paths. Seven modes cover clean cancellation, cancel publish dropped before
commit/acknowledgment lost after commit, run enqueue dropped/acknowledgment lost,
missed notification recovered at a 15-second virtual durable poll, and a stale
durable poll followed by a matching cancel. Every mode appears. First ten seeds
replay exactly; seed 42 produces identical traces in two separate processes and
is pinned as continuation-running-cancel-42.json in the regression corpus.

## Results

- 100,000 generated schedules PASS in 74.582 seconds: 100,000 scheduler choices,
  16,988,200 transport events, maximum virtual time 15,000 ms. This is a single
  workload's release-count proof, not the full release gate.
- 1,000-seed race workload PASS in 12.106 seconds.
- Complete pinned regression corpus under race PASS in 2.835 seconds.
- Real three-node race contract PASS in 49.719 seconds. Notification case:
  cancellation to terminal 116.826 ms, prefix 10/suffix 5, one effect, zero
  archive reads/two frame reads. Core subscription deliberately removed and
  flushed before the durable-poll case: 14.718 seconds, zero archive reads/one
  frame read. All three peers return ErrCancelled; raw-state audit passes.
- Compiled production generation-key mutation ignores the invocation sequence.
  The seeded helper fails immediately with `stale generation canceled effect`
  in 0.008 seconds. The real notification case under race fails with
  `stale-generation cancel interrupted active continuation` in 16.510 seconds.
  These are semantic failures after compilation, not timeout controls.
- Vet and diff whitespace checks PASS.

```sh
SIM_SEEDS=100000 SIM_COVERAGE_SUMMARY=1 go test ./sim -run '^TestSeededContinuationRunningCancelReplay$' -count=1 -timeout=10m -v
go test -race ./sim -run '^TestSeededContinuationRunningCancelReplay$' -count=1 -v
go test -race ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
go test -race ./integration -run '^TestContinuationCancellationInterruptsActiveSuffix$' -count=1 -v
go vet ./sim ./integration
```

## Limits

The simulator schedules modeled transport outcomes and virtual polls, not NATS
Raft, process SIGKILL or packet loss. The real proof is one cooperative blocking
effect and two cancellation delivery paths, with no concurrent server faults.
The two cancellation times are individual samples, not p99. Neither case proves
interruption of a user handler that ignores its context. Checkpoint publication
in this workload is clean; combined publication/GC/cancellation cuts and offline
replay of a terminal cancellation with a pending effect remain acceptance work.
The final-source full matrix/24-hour soak and online GC requirements remain open.
