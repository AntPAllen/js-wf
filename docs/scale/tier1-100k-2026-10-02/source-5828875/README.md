# Completed older-source 100k campaign

Run36962606650 at58288751c49bbb40c10214319842190a3d5eaa60 is terminal/success.
Original artifacts report154 top-level passes,234 pinned regressions, two
trace-only skips and10,700,632 schedules /174,556,738 choices /2,592,389,673 events.
The command ran6361.73 seconds. SIM_SEEDS=100000 is configuration evidence,
not independent per-workload seed coverage proof at this source. This revision
predates the113-workload/258-pin suite and independent seed-range accounting.
The latest-source100k requirement remains open. Original metadata and artifact
bytes are retained; no report has been upgraded to claim current coverage.

The current guard independently accepts the original inventory/event data.
Its rechecked report only adds `per_workload_seed_proof: null`; all original
fields match. That explicit absence is retained, rather than upgrading the run.
