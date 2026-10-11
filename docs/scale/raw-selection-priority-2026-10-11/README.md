# Selection priority proved by retained history — 2026-10-11

The independent raw auditor now checks selection priority before observers consume
signals or materialize selected promises. A timer/signal selection cannot choose
the timer when the requested signal is buffered and unused. Select-many cannot
skip an earlier case whose readiness is proved by the prefix: an unused delivered
signal, an explicitly established promise cache, or a matching live zero-deadline
timer. Ambiguous AwaitSignal/AwaitPromise candidates do not establish a cache.
All select-many case kinds/names and optional child identities are admitted,
including unselected cases. Existing timer-identity, selected-signal FIFO,
promise-provenance and checkpoint census checks still apply.

This uses own wire declarations and retained queues/cache/timer history, with no
SDK selection/replay or clock-readiness function calls. Positive-deadline timer
readiness cannot be established without delivery clock evidence and remains open.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphSelectionPriority|TestRawGraphSelectionReadiness|TestRawGraphCheckpointSDKSignalHistory|TestRawGraphCheckpointSDKTimerHistory|TestRawGraphCheckpointPromiseHistory|TestRawGraphRejectedLimitBoundary|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=3m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-selection-priority-native-audit-20261011 go test ./worker -run '^TestNativeGraphContinuationTimerHistory$' -count=1 -v -timeout=4m
```

Actual race exit0 in17.092s: six positive/six negative physical JSON/protobuf
fixtures first pass complete reference auditing with coherent hashes, edges,
object census and matching terminal projection. They cover argument reordering,
absent earlier signal, skipped earlier buffered signal and malformed unselected
cases. Fifteen component controls cover consumed signals, buffered/cached/ambiguous
promises, immediate/positive/cancelled/fired timers and timer/signal priority.
Component acceptance means only that readiness does not rule out the choice;
other full-path history checks can still reject dead handles or missing signals.
Signal/timer/promise history and rejected-limit regressions pass, as do four
native R1/R3 indexed/unindexed checkpoint cases in the race command.

The final native command exits0 in18.971s for all four R1/R3 × archive=false/true
production SDK timer-history cases. Four raw-audit receipts report40 entries,
one terminal, no pending invocation and two checked checkpoints each: eight
historical frames. Actual worker executable SHA/build and retained complete
server-storage census are recorded. This covers valid native timer selections;
priority corruption rejection is demonstrated by the physical/component controls.
The race executable was not captured before exit; its source manifest was prepared
after that run and before the native run. Final source hashes were verified
unchanged at close. This is focused evidence, not full frozen qualification.

The first native command omitted WF_GRAPH_SDK_RAW_AUDIT. Its actual exit0 in18.600s
is retained in initial-native.log with its executable and server stores; it is
runtime compatibility evidence only. The final command explicitly enables the
independent audit and its four receipts are required by review.json. Both closed
storage roots and every file/hash/mode/nanosecond mtime are listed in the compressed
inventory. No server restart or physical-power durability claim is made.

The new CI subset was extracted and executed against race.log, requiring every
physical/readiness control. Existing CI already enables raw auditing for native
timer-history cases. No hosted CI execution is claimed. The prior failed native
four-case run and its unconfirmed lease-initialization cause remain preserved;
this passing current-source run does not determine that earlier cause.

## Remaining scope

Positive-deadline clock/readiness and complete case ordering across uncertain
promise caches remain open, along with exact configured-cap exhaustion, full
operation execution equivalence and original simulation/scale/fault/soak/rollout
acceptance. Both larger frozen campaigns remain live under their original sources.
Public continuation admission/import/online collection remain disabled.
