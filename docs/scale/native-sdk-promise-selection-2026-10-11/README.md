# Native competing promises and cached outcomes — 2026-10-11

Eight production SDK cases cover R1/R3-domain, archive=false/true and successful/
failed child results. After the first checkpoint the child executes once, then
Select skips an absent signal and chooses the first of two ready cases referring
to that promise before a ready immediate timer. The timer loser remains usable
and is successfully awaited. After the second checkpoint the same competition
must choose the cached promise without consuming another signal. Success returns
the original large child result; failure preserves the planned child error.
Parent result remains 43, effects 2 and each parent stage runs once.

The retained logical journal independently confirms exactly two select-many
request/completion pairs: case 1 both times, first_signal>0 then cached_signal=0.
The raw auditor validates exact promise origin/outcome ownership, signal use,
timer history, known-ready argument order and both historical checkpoint frames.
Before final parent resume, the original child is retired. Archive cases sweep
both parent boundaries and reclaim original child terminal/result receipts;
parent-owned outcome bytes remain sufficient for cached reuse after that removal.

```sh
WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-native-promise-selection-20261011 go test -race ./worker -run '^TestNativeGraphContinuationPromiseSelectionPriority$' -count=1 -v -timeout=12m
```

Actual exit 0 in 215.258s. Sixteen priority receipts, eight cached-reuse receipts and
eight raw journal audits verify 16 checkpoint frames. Each audit reports one
35-entry parent journal, one terminal, no pending invocation and one retired child
projection. The actual live race executable SHA/build, unchanged 344 source-input
hashes, complete log and complete closed server-storage census (1892 files, 96,024,044 bytes) are retained (exact file
count/bytes in review.json). No frozen full qualification or hosted CI claim is
made. The new CI step requires every matrix case and all receipts; its exact
Python guard executed against this completed log.

Each new case has a two-minute functional context watchdog for complete child/
parent/collection work; the outer 12m watchdog bounds the eight-case process.
These are fixture lifetimes, not recovery-latency targets. Existing fixture modes
keep their prior watchdogs. Production defaults and latency gates are unchanged.

Positive-deadline clock/readiness, ambiguous promise-cache completeness, full
seeded/scale/fault/soak and original rollout/admission/import/online collection
gates remain open. These fixtures establish explicit cached success/error
selection after retirement/collection, not all-peer storage-power durability.
