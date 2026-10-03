# Native and fallback timer scan progress

Both original scanners at `f92ac5f` lose confirmed progress after later read
timeouts. The new fixture reproduces fallback seed 2 in 0.006 s and native
seed 14 in 0.004 s: eight failed attempts consume 48 virtual seconds, with
cursor 1 and no wakeup. The native reproduction uses archived original scanner
sources through a Go overlay and narrows the fixture's seed range to 14.

Both scanners now certify a completed prefix through `RetrySequence`. Native
timer journal/clock/retirement errors and uncertain wakeups keep the failing
invocation in the retry range. Fallback scans require both acknowledged wakeup
publication and successful source deletion before certifying that timer. Failed
or uncertain deletion remains a retry boundary, including retired generations.
The fenced loop renews ownership before checkpoint CAS and reloads uncertain
cursor writes. An acknowledged publication remains acknowledged in repair
observations even if the following delete fails.

## Verification

- All 1,024 fixed seeds and exact replays pass in 0.497 s. Forty-eight backend,
  policy/save/ack and fallback-delete cells are pinned, bringing the corpus to
  355 pins. The 121 scalable workloads are unchanged; these are fixed cases.
- Complete pinned corpus and the new model pass race instrumentation in 12.760 s.
- Fifteen failure-stage controls pass normally in 0.004 s. They cover first
  reads, hole metadata, journal/retirement/state/clock reads, uncertain publication,
  deletion, retired deletion, and publication evidence after a failed delete.
- Real three-node contracts pass normally in 7.078 s. Native cursor progression
  is `1 → 4 → 7 → 8`. Fallback is `1 → 4 → 7 → 8 → 8`, surviving an actual
  committed wakeup with a hidden acknowledgment and an actual committed timer
  deletion with a hidden acknowledgment. Both retain cursor 8 and one run message;
  fallback retains all seven future timers and removes only the due timer.
- Both real contracts and the fifteen controls pass race instrumentation in
  7.002 s. Full reconciler suite passes normally in 29.333 s.

## Scope and originals

The model charges sequential requests 100 or 150 virtual milliseconds within a
five-second attempt and uses the actual scanners, lease/cursor loop and retained
wakeup model. Native prefix journals are terminal; fallback prefix timers are
future. Lost enqueue and cursor acknowledgments, dropped deletes and lost delete
acknowledgments are replayed. Corrected cases finish below 30 virtual seconds
with one retained wakeup. The virtual lease TTL is 30 seconds; production is
12 seconds. Old-policy controls cannot reach configured save/enqueue/delete
faults; the deletion choice is unused for the native backend. The real contract
does not run a worker or prove full workflow completion or independent store
reopen. Population-independent scan latency remains unproven.

The archive contains both failed baseline traces/logs, original scanner sources,
the native baseline overlay and fixture, corrected source snapshots, focused
logs and all 48 pins. The first unused-import compile failure is preserved and
does not establish a runtime defect. Every archive member was reopened and
SHA256 verified before atomic publication. Actual executables and physical
stores are not retained by these focused runs. The race reconciler build used
an executable `/dev/shm` temporary directory to reduce disk pressure; its test
stores still used the ordinary disk-backed temporary directory.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./sim -run '^TestTimerPartialCursorReplay$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^(TestTimerPartialCursorReplay|TestPinnedRegressionCorpus)$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./reconcile -run '^(TestTimerPartialCursor|TestFallbackTimerPartialCursor)' -count=1 -v
```

For baseline reproduction, regenerate absolute overlay paths for archived
original `timers.go.txt` and `fallback_timers.go.txt`. Use the current fixture
for fallback seed 2, or archived `baseline-native-fixture.go.txt` for native
seed 14, and add `-overlay=<overlay.json>` to the first command.

Full current-source normal/race/100k, real matrices and 24h remain open. Earlier
complete-suite evidence keeps its earlier production graph scope.
