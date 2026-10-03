# Suspended scanner progress after partial failures

The original scanner at `1cfbd5986f711ccf8561584ed8c3fd490840f638` fails
the new deterministic fixture at seed 2 in 0.008 s: eight failed attempts
consume 48 virtual seconds, with cursor 1 and no acknowledged wakeup. It
discarded a confirmed prefix whenever later concurrent reads failed.

The corrected scanner certifies only a contiguous prefix whose reads,
retirement operations and required enqueues succeeded. Concurrent successful
reads beyond an unresolved invocation cannot advance the checkpoint past it.
An uncertain enqueue also remains unresolved, even when a later read fails.
Independently discovered ready wakeups retain their existing behavior. The
existing loop renews ownership before cursor CAS and reloads after uncertainty.

## Evidence

- All 128 fixed seeds and exact replays pass in 0.194 s. Twelve policy/save/ack
  cells are pinned: 307 total regression pins; 121 scalable workloads unchanged.
- Complete pinned corpus and the new model pass race instrumentation in 9.181 s.
- Eleven controls check first/later read failures, journal and timer retirement
  failures, earlier/later uncertain enqueues, successfully acknowledged prefix
  wakeups, proven holes, dry-run behavior and successful wrapping.
- Controls and the real three-node contract pass normally in 3.598 s and with
  race instrumentation in 4.602 s. Actual persisted cursors are `1 → 4 → 7 → 8`;
  a committed enqueue acknowledgment is hidden once, its retry succeeds, and
  exactly one wakeup remains in `WF_RUN`.
- Full reconciler suite passes normally in 24.734 s.

## Model and qualification scope

The deterministic fixture admits an immutable prefix of 96 or 128 sequences
per scan and rejects later reads with a deadline error. Concurrent read results
are independent of Go goroutine order. Each failed attempt charges five virtual
seconds; cadence is one second. This is a bounded admission model, not a model
of parallel request duration or arbitrary read completion interleavings. It
executes the actual production scanner, fenced loop and retained wakeup model.
The discarded-prefix control stalls; corrected cases acknowledge repair below
30 virtual seconds. The virtual lease TTL is 30 seconds; production is 12 seconds.
Configured cursor/enqueue faults are unused in controls that cannot reach them.

Both model and real contract keep enqueues within a single deduplication window.
The real fixture retains valid terminal journal prefixes and one continuation
suspension; injected read failures depend on sequence, not concurrent call count.
It proves cursor and enqueue semantics, not full workflow completion, arbitrary
parallel read schedules or population-independent latency.

The archive preserves original failure trace/log, corrected sources, original
scanner source, all focused logs, twelve pins and tracked patch. Every member was
reopened and SHA256 verified before atomic publication. These focused runs do
not retain test executables or physical stores, and do not independently reopen
stores. The [complete retained-binary normal 1k suite](full1k/) is independently
accepted at `27440da`: 172 passes, 307 pins, 121,000 bodies and 1,048 verified
source hashes. Current-source full race/100k, real matrices and 24h remain open.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./sim -run '^TestSuspendedPartialCursorReplay$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^(TestSuspendedPartialCursorReplay|TestPinnedRegressionCorpus)$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./reconcile -run '^TestSuspendedPartialCursor' -count=1 -v
```

For baseline reproduction, use a Go overlay replacing only the checkout's
`reconcile/suspended.go` with archived `baseline-suspended.go.txt`, then run
the first command. Keep the new fixture and other current sources.
