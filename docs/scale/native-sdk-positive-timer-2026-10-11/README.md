# Positive native scheduled timer through SDK continuations — 2026-10-11

Actual R1/R3Domain race cases run the real worker RunPartition consumer. The
initial handler cancels a one-hour timer, starts a75ms timer and suspends at Await.
NATS native scheduling publishes the due target delivery; no direct execute call
or manually supplied timestamp resumes this timer. Replay reuses its single
creation and completion, then publishes the next continuation. The next stage
cancels another one-hour timer and fires immediate SelectSignal/Select timers.
The finish stage returns43 with exactly two effects.

The test requires exactly one timer:initial-fire suspension, two named continuation
handoffs, one75ms timer declaration with a nonzero deadline, three acknowledged
schedules, initial handler count2 and next/finish counts1. These checks precede
an independently fenced terminal duplicate and the full raw journal/checkpoint
history audit. No second user effect occurs on timer replay.

## Evidence and original fixture correction

The initial native command finished exit1 because the new fixture counted every
Suspended record as a timer wait. Both workflows finished successfully, but the
two continuation handoffs made the count3. Production continuation.go explicitly
appends those handoffs; the corrected guard decodes waiting_on and requires all
three expected waits separately. The failed raw log, tested source and executable
receipt remain beside the final evidence. No runtime behavior or deadline was
changed to pass the corrected guard.

```sh
WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-positive-timer-final-20261011 WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^TestNativeGraphContinuationScheduledTimerFlow$' -count=1 -v -timeout=4m
```

review.json records the actual final exit/timing, native cases, raw audit receipts,
source hash checks, actual race executable and retained closed native stores. The
new CI step requires both complete cases and executes its guard against this log.

## Scope remaining

This proves positive-duration native delivery in the legacy server-timestamp
clock mode under healthy R1/R3Domain. It does not prove conservative tagged-clock
bounds, early/late or leader-change repair, ordered case competition, cancelled
wakeup no-op, process-kill cuts, archived positive timer history or original
scale/fault/soak gates. The earlier four-case native timer campaign remains failed
on unconfirmed lease initialization; this distinct run does not replace it.
Frozen159-family race and classified100000-entry campaigns use earlier sources.
Public admission, import and online collection remain disabled.
