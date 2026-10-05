# R1 direct audit cursor: actual R5 process loss preparation

The opt-in `TestDirectR1AuditR5ProcessOwnerLoss` creates two fresh five-container
fixtures with R5 file INV/JRN/STATE sources. Both publish and baseline-check exactly
1500 invocations / 6000 entries, including 24 MiB of request payloads. At journal
visit128 it verifies the actual named memory/AckNone R1 cursor owner and pending
tail, records its pre-kill log, and uses observed SIGKILL/source-exit proof.
One case leaves that owner down; the other restarts the same node/store.

The original20s audit deadline includes fault observation and restart. Full exact
reports, strictly contiguous once-only journal visits and zero INV/JRN consumers
are required. Fault result JSON is written before verdict assertions. No transport,
replay or retry allowance is changed; defaults are unchanged. Compile/opt-in skip
passes; actual process verdict and independent archive review are pending.

The retained producer captures clean selected Git Go/module inputs, actual race SDK
bytes/metadata and observed external server bytes/module/container/PID/mounts,
then verifies source and closure. This preparation does not qualify large fault
capacity, live traffic, legacy R1 configuration, current-source matrices or24h.

## Live observation at executed69f2dc7

Producer service `js-wf-direct-r1-r5-process-owner-loss-20261005.service` remains
active with actual SDK PID702605. First leave-owner-down audit returns exact
1500/6000 report in4.040516067s, no error, pending4039 at actual owner SIGKILL.
The fixture has not returned its final verdict or advanced to same-store restart.
Independent read-only standard NATS stream-info requests and `/jsz` observations
show zero INV/JRN consumer counts on allfour survivors. These are live metadata
observations, not a closed-fixture archive or parent qualification. The client/test
lifecycle blocking location and cause are unconfirmed; retain the same run through
its existing deadline. Reviewer service is pending terminal source/closure proof.
