# Original retained Tier-2 ten-minute worker pause qualification

Run seed1 of the original `TestMixedMatrixWorkerPausedFortyFiveSeconds` under its
normal 2 CPU/2 GiB profile with `run-tier2-retained-row.py --row pause --duration 10m`.
Ten confirmed 45-second SIGSTOP/SIGCONT pauses must hold actual leases, then produce
fencing tied to the paused lease after resume. Existing native mixed workload,
checkpoint/final integrity, histories, p99 and physical drain gates remain.

Reviewer checks actual SDK/source, external captured files, server and worker
executables/closure, all ten retained fault records and native duration acceptance.
Independent copied-store/history/drain review and whole archive preservation follow
closure. This is one original sustained seed; full13x200/current-source gates remain.
Long campaigns overlap; no isolated performance claims. Never reopen original stores.

The native reviewer also verifies complete ordered fencing records for each
actual PID/worker, requiring matching paused invocation key/epoch after resume.
Copied-review expected cardinalities must be saved from the independently reviewed
original MATRIX_RETAINED result; they are not fixed to another fixture.
