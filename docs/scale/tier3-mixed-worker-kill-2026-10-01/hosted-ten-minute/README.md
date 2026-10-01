# Hosted ten-minute R5 worker SIGKILL proof

[Run36923802476](https://github.com/AntPAllen/js-wf/actions/runs/36923802476)
is terminal SUCCESS at3a82c86a3d857e31a369835358e5f090c2f57fc9.
Named race PASS669.60s; package PASS670.625s. Twenty-eight batches complete
784 invocations with8,657 journal entries. All119 five-second SIGKILL slots and
replacement chains reverify from original downloaded artifacts:49 acquired
held-delivery kills and70 explicit no-new-acquisition selections. There are124
process generations; five surviving processes have matching graceful final
counter snapshots. No interrupted tails are observed. Killed generations have
no final snapshots, so complete hard-kill attribution remains false.

All histories, retained invariants, raw terminal/progress p99 and physical
WF_RUN/all64-consumer drain pass. Worst terminal p99 is17.919320144s; progress
p99 is13.244078307s. Production timing remains12s lease TTL,13s AckWait and3s
heartbeat, with R5 stores and2m write sync. Zero fencing records are retained.
All219 repairs are acknowledged (25start,162signal,32suspended) with checked
source/decision explanations. Zero recorded fencing is not a complete counter
claim for killed processes. Held cuts occur at acquisition; this does not prove
all in-flight effect/continuation/process-fault combinations.

This closes one ten-minute row at its source, not the200-seed matrix or full24h
Tier3 release gate. Original hosted server stores are absent from the download.
Large raw files are losslessly compressed. The timeline reviewer reuses original
retained events and cannot infer an unobserved broker commit or server cause.
