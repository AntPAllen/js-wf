# Full 120-workload default 1k gate accepted

The complete simulator suite passes at exact
`3a5452e14924e413c10ed1504c7da2a55549b196` in 120.296 seconds. Independent suite
checking verifies 164 top-level passes, two documented trace-only skips,
266 regression pins and exactly seeds 1..1000 for each of 120 source-inventoried
workloads: 120,000 actual bodies. Aggregate coverage is 122,034 schedules,
1,797,015 choices and 27,138,160 transport events.

Every recorded Go/module file hash matches the Git revision and the worktree
after execution. Compiled and AST inventories were generated from identical Go
sources at a8e7670 before documentation-only commits; their source hashes are
unchanged. Regression paths and every pin's bytes independently match Git.
The suite-result guard checks actual completed ranges, every named verdict,
every pin and the terminal package result. This qualifies the final helper's
failed-trace preservation change as well as the full new workload graph.

The earlier whole-package race attempt at a8e7670 timed out after 900.076 seconds
inside the older `TestSeededSuspendedScanCapacityReplay`, with 114 top-level
passes and no package completion. It is rejected and retained alongside the
accepted standard run. The normal command is the existing per-commit/release
suite mode; focused race tests and compiled controls for the new model were
already accepted separately. No whole-package race pass is claimed.

All 22 accepted/rejected original evidence files are losslessly archived with
SHA256 readback before atomic publication. See `manifest.json` and the accepted
`source-regression-hashes.json` member. Originals remain in
`/tmp/js-wf-tier1-120-1k-default-3a5452e-20261002` and
`/tmp/js-wf-tier1-120-1k-a8e7670-20261002`.

This clears the current default 1k gate, not the 100k release seed gate, full fault
matrix, million-timer physical drain or full-runtime 24-hour soak.
