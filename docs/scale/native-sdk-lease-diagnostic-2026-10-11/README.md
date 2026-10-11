# Native SDK lease diagnostic — 2026-10-11

The original four-case timer campaign failed R3Domain/archive=false during lease
initialization, before user code (see ../native-sdk-timer-history-2026-10-11).
That failure remains unaccepted with cause unconfirmed.

An instrumented single R3Domain/archive=false diagnostic passes race22.738s,
actual exit0. Native Create/Update observations show four acknowledged Create
revisions1/25/54/61, with initialization CAS against exactly that revision. The
full SDK timer/continuation/raw history checks pass. This is not a production fix
or replacement four-case qualification. No native executable receipt was captured
before this short diagnostic ended; source hashes and complete raw log remain.

## Instrumentation and retained stores

The test wraps the real KeyValue in a delegating observer; the production lease
adapter still interprets errors and makes every acquisition decision. It logs
Create and Update revisions, values, durations and errors. On errors a fresh
three-second context captures public KV_WF_LEASE StreamInfo and last lease subject
message. It does not retry or change outcomes. A positive result cannot establish
whether added timing changes the failure probability.

WF_GRAPH_SDK_DIAGNOSTIC_ROOT preserves native file stores after test shutdown.
The closed diagnostic store retains423 files/11940128 bytes, with full hashes,
modes and mtimes in retained-store-inventory.json. No files were moved or removed.
Only test-supplied non-secret workflow identities/values are logged. The current
successful log spells nil errors as Go's formatting marker; this is cosmetic.

```sh
WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-timer-lease-diagnostic-20261011 WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^TestNativeGraphContinuationTimerHistory$/^R3Domain$/^archive=false$' -count=1 -v -timeout=3m
```

## Deterministic safety control

TestInitializationConflictPreservesSuccessor passes race1.018s, actual exit0.
An in-memory KV transport inserts a successor between Create and initialization.
The original acquisition must return ErrLost plus revision-mismatch with no owner,
no initialization retry and no successor deletion. A later acquisition must return
ErrHeld and preserve exact successor bytes/revision. This verifies client safety
under a possible conflict; it does not establish the cause of the native failure.
CI requires this control. No new seeded Tier1 family is claimed.

Original timer four-case, positive-deadline/clock, full current-source simulation,
scale/fault/soak and other implementation-plan acceptance gates remain open.
