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
