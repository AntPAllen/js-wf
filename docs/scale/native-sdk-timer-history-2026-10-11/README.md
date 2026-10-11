# Native SDK timer history through continuations — 2026-10-11

The four-case actual race campaign attempts R1 and R3Domain with archive=false/true. Production
SDK handlers create a one-hour timer and cancel it, then create and Await an
immediate timer in the initial stage. The next continuation creates/cancels
another one-hour timer and fires two immediate timers through SelectSignal and
Select. The finish stage returns43; every handler runs once, with two effects.

The first checkpoint cancellation set must equal[4]; the next must equal[4,14],
using absolute SDK positions after restoration. Worker annotations, native owned
frames and the complete raw history auditor verify this state. Archive cases
sweep after each checkpoint, prove removal of original physical entry receipts,
read the complete relocated history and resume from the owned frame. The final
independent auditor walks both archived prefixes and the live suffix.

## Evidence

Executed command:

```sh
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^TestNativeGraphContinuationTimerHistory$' -count=1 -v -timeout=8m
```

review.json records actual exit/timing, all four native cases, exact checkpoint
and raw audit receipts, captured source hashes and actual race executable.
The exact new CI guard is executed against the native log and retained here.

## Campaign failed; three cases passed

Actual child exit1. Both R1 cases and R3Domain/archive=true passed. The R3Domain
case without archive failed lease acquisition/epoch initialization after1.79s,
before the user timer handler: NATS error10164, wrong last sequence / key revision
mismatch. Cause is unconfirmed. This run does not qualify all four cases. The CI
guard was executed and rejects the incomplete campaign. No retry, gate relaxation
or production workaround was applied. Raw log, source hashes and actual executable
receipt are retained; temporary server stores were removed by Go test cleanup and
are unavailable for server-side diagnosis.

## Scope still open

Positive-duration timers here are scheduled and cancelled; fired timers have zero
duration. This does not prove a positive-deadline wakeup, clock lower-bound
readiness, ordered case competition, process-kill cuts, physical timer mutations
or full original scale/fault/soak acceptance. The frozen159-family and classified
100000-entry campaigns predate these tests and remain separate qualifications.
Public admission, import and online collection remain disabled.
