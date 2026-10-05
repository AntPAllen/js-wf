# Recorded automatic-membership seed1 failure and parent-stack diagnostic

Run37164231641/job111324441062 at79915ca fails seed1 after885.82s;
seeds2–13 do not execute. Original600s workload,512MiB/default race profile,
audit/latency/completion bounds and failed parent verdict are unchanged.
Artifact11306811431 ZIP751300 bytes hashes to
`0247b4a969c7a6df308b420519a9eb88c915fa8eaea21c93c9b119dad8a046ae`.
All82 uploaded raw files match the ZIP; complete raw/API/logs and scoped
analysis are preserved in the readback-verified archive.

## What the uploaded evidence establishes

`matrixfanout/tier3-1-batch-78-9` Start acknowledges invocation2194 at
14:44:58.798Z; Await times out five minutes later. The identity maps to
partition31. Its captured successful assignment CAS chain ends at revision340,
owner `tier3-mixed-1`, before that Start. That owner's last captured session
rejoins at epoch1368 at14:44:25.929Z. There is no dispatch record for the target
in the uploaded file. Four missing-journal repair attempts acknowledge dispatch
publication afterward. The most recent captured assignment is not an independent
final retained-state observation.

No final consumer/assignment snapshot, workload SDK/full captured source or
physical store payload is uploaded. The raw evidence cannot distinguish an
assignment watcher/local dispatch-loop wait from a native consumer/delivery
problem. It does not establish a NATS cause, a runtime defect or a repair.
All original logs, including the terminal failure, remain unchanged.

## Diagnostic preparation

The R5 harness now captures its parent's goroutines at an active-workload failed
boundary before cleanup joins. The artifact includes PID/time/bytes/truncation;
only the parent SDK process is represented. Automatic-membership workers run in
that parent; subprocess workers in other rows are not represented. Capture grows
from64KiB up to32MiB and reports truncation at the limit. Healthy workloads emit
no failure stack. This does not change registration, assignment, dispatch, fencing
or any audit/latency/deadline acceptance rule. Startup failures before installing
this defer are outside the capture scope.

A real blocked parent goroutine is retained by the control; normal/race pass
0.008s/1.071s. A Go overlay restricting capture to the caller fails0.010s with
`blocked parent goroutine absent from stack`. Source hashes are captured after
these commands; control binaries and temporary stacks are not retained and no
before/after execution-source ledger is claimed. This proves diagnostic behavior,
not native automatic-membership recovery. The isolated row producer now requests
and asserts a clean revision-stamped actual SDK build for future runs.

Full/current-source automatic membership, original failed-campaign cause and
full release matrices remain open. A fresh ten-minute native diagnostic will
retain actual source/binary/store identity; any newer-source pass cannot explain
this older-source failure.
