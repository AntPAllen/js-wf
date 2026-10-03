# Tombstone scanner progress across timeouts and uncertain deletes

The original production scanner had no certificate for partial progress, and
the tombstone loop discarded every failed pass. A neutral port refactor at
`485604f` exposes the same per-pass handles and decisions to the simulator;
the original real paging/newer-state protection test passes in 0.075 s.
Baseline seed 2 then fails in 0.007 s: eight five-second attempts plus cadence
consume 48 virtual seconds, cursor stays 1 and the expired target remains.
Original pre-refactor and ported pre-fix sources are retained together.

The scanner now certifies only completed stream-sequence prefixes. Read,
generation lookup, decode and uncertain delete failures retain the unresolved
sequence. Holes, reserved keys, absent or newer revisions and completed sweep
decisions can advance. The production tombstone loop propagates this certificate
through the existing fresh-ownership renewal and cursor CAS path.

## Evidence

- All 256 fixed seeds and exact replays pass in 0.220 s. Twenty-four
  policy/cursor-save/delete cells are pinned: 379 total pins; the same 121
  scalable workloads. These are fixed regression cases, not another scalable
  workload.
- Complete pinned corpus and the new model pass race instrumentation in 9.583 s.
- Thirteen read/info/value/decode/generation/delete and prefix controls pass
  normally in 0.005 s and under race instrumentation in 1.019 s. They include
  reserved keys, holes, absent/newer revisions, dry-run protection and completed
  prefix deletions.
- Actual three-node contracts pass normally in 6.682 s and under race
  instrumentation in 7.508 s. Both dropped-before-commit and committed/lost-ack
  deletes follow scan cursors `1 → 4 → 7 → 8`, then retain cursor 11 after
  confirming the target and adjacent history holes. Dropped delete retries its
  original revision; lost-ack delete retries the target read and observes absence.
  Both commit exactly one delete and retain all seven protected result values.
- Full retention/reconciler suites pass normally in 0.254 s and 33.568 s.

## Model and qualification scope

The actual production scan and tombstone loop execute against modeled KV and
invocation transports. Cursor saves share the scanner's underlying state bucket
and revision space. Requests cost 100/150 virtual milliseconds within a 5 s
attempt. Prefix fixtures include a reserved cursor key, a hole, a superseded
revision, a future tombstone and a held invocation. Target deletion can acknowledge,
drop, lose its acknowledgment or race a newer generation/state update; revision
CAS preserves the replacement. All protected values remain byte-identical.
Corrected cases resolve the target below 30 virtual seconds. The old-policy
control discards partial-prefix metadata before using the same fenced loop;
configured save/delete faults are unused when it cannot reach those operations.

The History=1 model represents deleted markers as holes rather than explicit
marker messages. Both permit safe advancement but are different read paths;
unit/real contracts cover current-key absence. The virtual lease TTL is 30 s,
production TTL is 12 s. This proves cursor/sweep semantics, not workflow completion,
arbitrary server scheduling, independent store reopen or population-independent
cleanup latency. Physical stores and focused executables are not retained.

The first fixture verification loop traversed a Go map without sorting and
failed exact replay. Its original source/log is retained; sorted verification
fixes the fixture. That rejection is not evidence of a runtime defect.

All focused logs, original failure trace, source snapshots and 24 pins are in
the archive. Every member was reopened and SHA256 verified before atomic
publication. The RAM build cache was reused through a per-command `GOCACHE`.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./sim -run '^TestTombstonePartialCursorReplay$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^(TestTombstonePartialCursorReplay|TestPinnedRegressionCorpus)$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./retention ./reconcile -run '^TestTombstonePartialCursor' -count=1 -v
```

For baseline reproduction, overlay `retention/tombstone_scan.go` and
`reconcile/tombstones.go` with archived ported baseline sources, then run the
first command with the new fixture. Full current-source normal/race/100k,
real matrices, 24h and original scale requirements remain open.
