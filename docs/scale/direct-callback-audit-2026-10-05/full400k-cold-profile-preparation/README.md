# Current-source cold full-capacity CPU/GC diagnostic preparation

Fresh full original donor copies only; prior failed copy stays closed. Same
400k/4.8M source cardinality,4CPU/GOGC500/4GiB, original20s including zero-consumer
cleanup and unchanged recovery semantics. No warmup or smaller population.

Optional `WF_AUDIT_CAPACITY_R1_CPU_PROFILE=1` captures CPU profiles with stream
labels, per-stream start/end (no per-record clocks), before/after heap/NextGC,
GC count/pause, and actual watch creation/Stop lifecycle without update relays.
WatchStop alone does not prove initial-set completion. Runtime counters include
profiling overhead and shutdown; total deadline verdict is captured before CPU
profile flush so artifact work cannot redefine the audit gate. Report/count/error
and whole closed fixture must be preserved for either verdict.

Constant stream identity lookup hoisted outside per-record callback;500ms
observer is sufficient for owner-left-down (no short-lived successor to capture).
This changed diagnostic will separate integrated current-source cost from the
previous ordered warmed capacity result; it does not assume GC/observer causes.
Compile/opt-in skip passes; execution awaits verified old-copy reclamation and
new fresh-copy/source/actual process/closure/delta verification.
