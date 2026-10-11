# Native SDK priority with competing ready cases — 2026-10-11

A production SDK continuation fixture now exercises selection priority with
competing ready cases in R1 and R3-domain layouts, each with/without archive
collection. An unread signal is carried through the first checkpoint. In the
next stage it wins SelectSignal against a newly created immediate timer. That
timer remains available: Select skips an absent signal and picks it before a
second ready immediate timer. The losing timer is then successfully awaited.
After the second checkpoint, Select skips an absent signal and picks the first
of two ready cases with the same signal name. Payloads must remain 11 and 13.

The existing fixture also checks result 43, two RunOnce effects, one invocation
of each stage, an independently fenced duplicate, canonical start/signal/state
ownership and recovery after dropped run messages. Archive cases sweep original
entry receipts at both boundaries and resume through the archived checkpoint
history. The independent raw auditor must accept both frames and exact signal/
timer histories after all competing selections have completed.

## Execution and evidence

```sh
WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-native-selection-priority-20261011 go test -race ./worker -run '^TestNativeGraphContinuationSelectionPriority$' -count=1 -v -timeout=5m
```

Actual exit 0 in 107.358s; review.json records the completed run. Complete race log,
source manifest, actual live race executable SHA/build receipt and closed native
server-file census are retained. Every R1/R3 × archive=false/true case is required.
Eight priority receipts cover both continuation stages; four raw-audit receipts
must each report one journal with 34 entries, one terminal, no pending invocation and two
checked checkpoints: eight frames in total. Both archive stages in both archive
cases must report removal of original entry receipts.

The new CI step requires all four cases, all priority receipts and independent
raw/history audit receipts; the exact new Python guard is executed locally
against this completed log. The CI job supplies WF_GRAPH_SDK_RAW_AUDIT=1. This
is focused native race development evidence, not a hosted CI result or replacement
for full seeded/race qualification. Stores were closed and retained; their census
does not demonstrate all-peer power-loss durability or restart recovery.

## Remaining scope

Positive-deadline readiness needs delivery clock evidence. Native competing
promise cases, ambiguity in inferred promise caches, timer repair/clock-domain
faults and the original scale/fault/soak/rollout gates remain open. The larger
frozen campaigns keep their original source/invocation; public continuation
admission/import/online collection remain disabled.
