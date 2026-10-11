# Native signal binding commit failure — 2026-10-11

A bounded R3 canonical active10/10/1 race diagnostic on frozen Git source
`1b7a4bf62c460f9c28e2cf76b9ee58f769934b4c` reproduced the signal failure with
qualified phase labels. It completed 10 old invocations, kept 10 handlers active,
and overlapped purge of 10 IDs with reuse of one. The first reported failure was:

```
release Signal purge-scale/live-00004 invocation=11 (parent context: <nil>):
signal publish outcome unknown: bind ordered signal through 6:
commit signal binding: journal append outcome unknown: context deadline exceeded
```

The actual SDK exited 1 after 69.415 seconds; the test body reports 69.31 seconds.
The raw final audit was not reached. The parent context was healthy at this
reported failure. This is not a passing native race or full-scale qualification.

## What this establishes

The failure reached ordered signal binding commit, rather than reservation
preparation or signal binding preparation. It does not establish whether the
commit's authority observation, retained-graph verification, root CAS, or
readback failed, whether the candidate persisted, or a NATS server cause.
Commit includes checks before root CAS; a commit-phase error alone does not
prove that CAS was attempted. The same existing 3-second ordered binding attempt
and original unknown-outcome rules were used. No retries, deadlines or target
counts were changed to make this diagnostic pass.

The next investigation should inspect a verified copy for this exact input,
source and binding, then instrument commit operations if required. Recovery must
use the original retained token; an unknown reply does not authorize recreating
or retrying the request as a fresh signal. Previously qualified aggregate-budget
component controls remain separate from this unconfirmed native cause.

## Retained evidence

`run.py` starts one diagnostic, captures its actual executable through the
retained capture helper, records exact environment/driver/SDK exits and hashes,
and inventories the closed native store. `source-inputs.json` binds 2903 tracked
Go/Python/YAML/module inputs; the evidence scripts in this new directory are
separate. `review.py` independently checks each input against both Git source
and current bytes, executable hash and race instrumentation, actual terminal
exits, requested count and active mode, failure log, and every retained file.
`review.json` verifies closure, while explicitly refusing full-scale acceptance.

The original store has 2158 files / 14,345,960 bytes under
`/home/exedev/js-wf-native-signal-phases-20261011/native-active10-race`.
Executable and driver receipts remain alongside it. No original store was
opened after closure or deleted. No successful result or raw audit is inferred
from intermediate progress logs.
