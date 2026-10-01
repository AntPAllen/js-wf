# SDK continuation request/completion protocol

The modified tree based on `96af208` implements `wf.Continue` for contexts whose
caller supplies stage registration and authoritative journal/lease anchor facts.
Ordinary production workers do not configure those hooks yet; invoking Continue
there returns ErrContinuationUnsupported. No worker-dispatch completion claim
is made.

Continue validates stage and locals before publishing a normal checkpoint
request. It captures a bounded frame, always stores it under its content hash,
reads back exact bytes, then appends a result-ref completion. The worker receives
metadata for manifest publication only after the pair is verified. Continue
returns ErrContinuation and marks the delivery ended. CheckComplete cannot turn
that boundary into a successful terminal result. Once a publication fails, that
context cannot append further SDK steps or return normal completion.

A recorded completion uses its recorded index/epoch/attempt facts, not the
replacement owner's new epoch. Replay compares its stage/locals declarations
and rebuilt materialized frame with the recorded frame, loads it without storing
again, and suppresses prefix effects. A pending request can complete under a
higher epoch; a previously written uncommitted frame may then become an orphan.
User locals are marshaled once, and the transport receives detached frame bytes.

Proofs:

- SDK publication-cut controls under race: dropped and hidden request replies,
  hidden frame writes, frame-read timeout, dropped and hidden completion replies.
  Recovery leaves one logical request/completion pair, preserves a committed old
  epoch or uses a higher epoch for a new completion, and replays with zero stores.
  Additional controls cover changed stage/locals before I/O, unknown stage,
  live handles, nested runtime objects, serialization count and transport mutation.
- Full SDK race suite: 11.750 seconds. Focused combined SDK/model race: SDK
  1.024 seconds; model duration is in model-race.log.
- Eight seeded modes run production Continue, journal Append/Read, frame object
  transport, snapshot publication/purge and prefix-free restoration. Exact and
  cross-process replay passed. 100,000 schedules, 100,000 choices and 5,962,551
  transport events passed in 30.102 seconds (zero virtual time).
- Real three-node R3 file-store contract under race: 4.396 seconds. Every journal
  append renews a real lease. It hides a committed completion reply at the SDK
  appender boundary, releases that lease, acquires a higher fencing epoch and
  replays with no prefix effect/store repetition. The original completion epoch
  survives. Archive/runtime publication and purge then permit an archive-denied
  resume with saved state and a distinct next-stage external deduplication key.
  This is an injected SDK-boundary reply loss, not a network proxy or server bug.
- Final complete simulator suite including the new seed-42 pin: 87.821 seconds.
  Vet and diff checks passed.
- Removing the production store-input clone makes the transport-mutation control
  publish ErrContinuation over corrupt frame bytes and fail its assertion. The
  overlay compiles and runs; it was not applied to the worktree. Passing controls
  are in the race logs.

Commands: `go test -race ./wf -count=1 -v`,
`go test -race ./wf ./sim -run 'TestContinue|TestSeededContinuationSDKReplay' -count=1 -v`,
`SIM_SEEDS=100000 SIM_COVERAGE_SUMMARY=1 go test ./sim -run '^TestSeededContinuationSDKReplay$' -count=1 -v`,
`go test -race ./integration -run '^TestContinueUsesRecordedEpochAfterHiddenCompletionAck$' -count=1 -v`,
and `go test ./sim -count=1 -timeout=5m`.

Remaining: production worker registry/dispatch, append counters and runtime facts
for suffix execution, disabling generic compaction for those workers, suspension
and handoff recovery, offline multistage replay, retirement/reuse and process-kill
contracts. This does not close the mixed seed 65 miss or full release gates.
