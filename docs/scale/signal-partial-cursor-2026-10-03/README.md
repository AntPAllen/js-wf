# Signal scanner progress after partial timeouts

The production signal scanner repeats its confirmed prefix after every failed
pass. The final deterministic fixture with the original `cb7c86a` scanner,
substituted through a Go source overlay, fails seed 2 in 0.005 seconds: eight
attempts consume 48 virtual seconds, the cursor remains 1 and no wakeup is
acknowledged. The original first reproduction has the same result and is retained.

The corrected scanner returns `RetrySequence` only after completing a contiguous
prefix. The existing fenced loop renews ownership before checkpoint CAS, then
reacquires and rereads after uncertain saves. Journal reads, invocation lookup,
hole metadata reads and uncertain enqueue replies retain the failing sequence.
Completed or consumed signals, absent invocations and stale generations can
advance the prefix. Successful pass and wrap behavior is unchanged.

## Evidence

- All 128 fixed seeds and exact replays pass in 0.063 seconds. Twelve policy,
  cursor-save and enqueue-ack cells are pinned, increasing the corpus from
  283 to 295 pins. The 121 scalable workloads are unchanged. These fixed seeds
  are not another 100,000-seed workload.
- Both partial-prefix policy controls execute the actual scanner and fenced
  loop with immutable reads, virtual 100/150 ms request cost and 5-second
  attempts. The discarded-prefix control stalls; corrected cases acknowledge
  repair below 30 virtual seconds. The retained signal transport verifies exactly
  one wakeup despite a committed enqueue with a lost acknowledgment. Configured
  save/enqueue faults are unused in controls that never reach those operations.
- Complete pinned corpus and the new model pass race instrumentation in 8.117 s.
- Six failure-stage controls and the three-node contract pass normally in
  3.382 s and with race instrumentation in 4.544 s. Actual persisted cursors
  are `1 → 4 → 7 → 8`; the committed enqueue acknowledgment is hidden once,
  its retry succeeds, and one wakeup remains in `WF_RUN`.
- Full reconciler suite passes normally in 17.595 s.

The first real fixture attempt failed journal integrity checking because it
provided a `Completed` entry without `Started`. The corrected fixture retains
valid two-entry terminal prefixes. The rejected source and log are preserved.
This failure does not establish a runtime defect.

## Scope and reproduction

The archive contains all focused logs, both failing traces, corrected source
snapshots, rejected fixture source, original scanner source, overlay description,
and twelve pins. Every archive member was reopened and SHA256 verified before
atomic publication; the adjacent manifest records each hash. Actual test binaries
and physical stores are not retained by these focused runs. The real contract
checks cursor and enqueue semantics, not full workflow completion or independent
store reopen. The virtual lease TTL is 30 seconds; production TTL is 12 seconds.
This does not establish latency independent of retained population or identify
the earlier R5 server-side cause.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./sim -run '^TestSignalPartialCursorReplay$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^(TestSignalPartialCursorReplay|TestPinnedRegressionCorpus)$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./reconcile -run '^TestSignalPartialCursor' -count=1 -v
```

To reproduce the baseline, extract `baseline-signals.go.txt`, regenerate an
overlay mapping the checkout's absolute `reconcile/signals.go` to that file,
and add `-overlay=<overlay.json>` to the first command. The final model and
other source files stay corrected; only the scanner reverts.

Full current-source normal/race suites and extended seeds remain open. Queued
runs at `0d3fabf` precede this signal correction and retain their narrower scope.
