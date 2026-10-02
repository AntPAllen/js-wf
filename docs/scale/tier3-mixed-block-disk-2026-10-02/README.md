# R5 private filesystem stall fixture

The new `block_disk` row in `tier3-mixed-journal.yml` mounts node four's
persistent `/data` from a private 512 MiB sparse loop/device-mapper/ext4
filesystem. Other nodes retain independent ordinary stores. The filesystem
remains mounted until every container has stopped; each fault resumes its
mapping even on cancellation, and the fault goroutine joins before cleanup.

Every 30 seconds the row observes Docker's writable bind and elects both
`WF_RUN` and `WF_JRN` leaders onto that node. It records both R5 role observations,
suspends the private mapping for five seconds, and requires a dirty-file sync
on that same filesystem to remain blocked until resume. The row waits for R5
readiness after resume. The mixed workloads, raw enabling-event p99 <30s,
histories, immutable terminals, retained journal audits and physical dispatch
queue drain use the existing common fixture. File-store sync remains `2m`.

The independent row guard joins the mount, roles and blocked-sync timestamps
and rejects missing, short, escaped, mismatched or reversed evidence. Nine Go
bind validation cases reject unintended/shared paths; their race run passes
in 1.018s. All 34 Tier 3 Python tests pass, including 14 block-stall evidence
cases. The integration package compiles; its opt-in native test skips without
`WF_TIER3_MATRIX=1`. Those checks do not prove native fault acceptance.

The [clean hosted 35-second smoke](https://github.com/AntPAllen/js-wf/actions/runs/36944821596)
was dispatched at source1828d930243961232d0c2bdaa479fc2efee7eb3b;
`smoke-launch.json` retains the original queued API response. The completed
smoke and independent checks are described below. Sustained runs require that
smoke's original evidence to pass the row guard and event reviewers first.
This models actual blocked I/O, not per-request `dm-delay`, server process
pause, power-loss durability, or all in-flight operation combinations. The
200-seed full matrix and 24-hour full matrix remain open.

## Hosted smoke verified

[Run36944821596](https://github.com/AntPAllen/js-wf/actions/runs/36944821596)
passes75.33s test /76.349s package at1828d930243961232d0c2bdaa479fc2efee7eb3b. The actual private-device
capability test passes before the R5 row. The row completes nine batches,
252invocations and2774journal entries with one admitted node-four stall.
Mapping suspension lasts5.024863770s; the actual dirty sync returns5.028883410s
after suspension. Both admitted R5 leaders are on the writable private store.
Worst raw terminal p99 is4.993929649s and worst progress p99 is0.653438250s.
All histories, final retained audit, immutable results and physical dispatch
drain pass. Independently rerunning the current row guard, event explainer and
fencing reviewer passes. All35repair records are acknowledged (27signal,
8suspended); the five final worker counters match, and no fencing is observed.

`hosted-smoke/` retains every original downloaded artifact plus full job log,
terminal run API record and independent reviews, compressed losslessly with
original SHA256/byte manifests. It cannot independently re-audit deleted server
stores or certify underlying hardware durability. This is one35s smoke; a
sustained ten-minute row and the full matrices remain required.

The [ten-minute sustained row](https://github.com/AntPAllen/js-wf/actions/runs/36946199913)
was dispatched after verification at31619b5430a5a44cc5978947d1c739b9dc0065a1.
`ten-minute-launch.json` retains the queued API response. That run needs19
admitted stalls and all unchanged workload/audit/history/latency/drain gates;
there is no sustained verdict yet.
