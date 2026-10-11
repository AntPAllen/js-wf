# Retained R3 Start inspection and error phases

The full canonical 10k purge/reuse run failed during the initial Start cohort.
Its error grouped reader acquisition, input consumption and reader release under
`input`. That error does not establish which phase timed out or a NATS cause.

## Closed copied-store experiment

Before opening the cluster, every file in a reflink copy matched the original
closed census: 2443 files / 198,307,813 bytes. A public-API helper attempted to
inspect Start descriptors and recover pending ones with their original tokens.
It did not start workers or call ordinary Start with replacement payloads.
The helper reached its three-minute inspection deadline, actual exit 1, with
`context deadline exceeded`. It produced no successful recovery report. The
unlabelled inspection error does not identify the failed API or prove recovery.
No recovery count or full-scale acceptance is inferred from this experiment.

`review.py` verifies the actual helper exit, its retained binary, 1160 exact
pre-change Go/module inputs, the unchanged original store, and the final copied
store census. The modified copy has 2472 files / 127,140,525 bytes. Server reopen
and inspection can change storage; these file counts are not workflow counts.
Both copies and the helper/source snapshot remain under
`/home/exedev/js-wf-canonical-start-retained-recovery-20261011` and the original
full-run root. Initial compilation failed because the external helper imported
an internal package; both original and corrected build receipts are retained.

## Runtime and fixture diagnostics

`ReadStart` now wraps errors with the operation that failed: opening the reader,
reading Start state, reading input, or releasing the reader. Wrapping preserves
`errors.Is`, successful reads still fail on unresolved release, and a primary
read error remains primary when cleanup also fails. Existing exact-head readback
may resolve a committed release. Deadlines and retry rules are unchanged.

The scale fixture reports the old/live/reused cohort, workflow type/ID and parent
context error alongside a Start failure. This identifies future failing requests;
it cannot recover the identity omitted from the original full-run log.

Seven in-memory component controls check valid reads, opening failure, input
failure, release before commit, committed release with exact readback, committed
release without readback, and simultaneous input/cleanup failure. They check
error phase/cause, stable Start metadata and final reader-pin outcomes. The first
test run incorrectly expected an error after a committed release with successful
exact readback; its failure is retained. The corrected controls require success
for that case and failure for unconfirmed release.

All seven controls pass in both normal and race modes, actual exit 0 for each.
Normal/race command logs and actual exits are recorded separately. These commands
use ordinary `go test`; their ephemeral test executables were not separately
captured. `diagnostic-inputs.json` binds the changed sources and CI workflow.
CI includes both modes and requires all seven controls; its 14 embedded Python
blocks compile locally. No hosted execution or full source qualification is
claimed. Seeded inventory remains 163 families / 946 pins.

Next: isolate the copied R3 reopen/inspection deadline with per-operation
observations, then demonstrate pending-Start recovery and diagnose the original
full-scale failure. The 10k purge/reuse gate and all other release gates remain
open.
