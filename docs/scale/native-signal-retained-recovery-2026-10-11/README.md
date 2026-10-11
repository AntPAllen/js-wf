# Copied native signal recovery and finite deduplication — 2026-10-11

Two independently verified copies of the failed R3 active10 store inspect the
exact `purge-scale/live-00004`, invocation 11, `release`/`release-4` request.
The original 2158 files / 14,345,960 bytes remain unchanged and unopened.

Both restored copies have the original reservation token and owned body `4`,
matching physical source sequence 6, but no binding before recovery. This
establishes the restored committed state; it does not trace the original commit
RPC or prove the server-side reason for its timeout. No worker is started on
either copy, so this is not a full workflow execution/recovery qualification.

## Recovery outcome and retained failed assertion

The first helper recovers the original token in 98.152 ms. It returns sequence 6
and creates the matching first binding while preserving the input and original
source. It then exits **1** because its assertion forbids any additional physical
source. That assertion was stronger than the protocol's first-sequence guarantee.
The complete original helper, observations, executable and failure are retained;
its recovery is not presented as a passing helper run.

The second helper starts from another fresh byte-verified original copy. It
records that the source age exceeds the restored stream's 120-second duplicate
window. Recovery of the exact token completes in 191.360 ms and returns sequence
6. It creates one wakeup (WF_RUN messages 3→4) and one exact-token physical
duplicate (WF_SIG messages 12→13), whose sequence is 13. Processing that duplicate
advances the source frontier without replacing the binding at index 0/sequence 6.
The next bind reports no progress; descriptor, owned body and original source
are unchanged. The helper exits **0**. Physical republishing after finite broker
deduplication expiry is compatible with the durable first-sequence binding.

Both probes retain the initial restored node0 authority StreamInfo 8-second
deadline. They recheck read-only metadata in bounded rounds before graph access;
all subsequent probe phases succeed except the first helper's final strict
assertion. Socket startup alone is not treated as metadata readiness. These
startup observations do not assign a cause to the earlier signal commit timeout.

## Deterministic controls

`TestCanonicalSignalRecoveryAfterDedupExpiry` checks rejection versus committed
unknown binding outcomes at virtual times 119999 ms and 120000 ms. Within the
window recovery deduplicates the physical source; at the exact expiry boundary
an absent binding permits a duplicate source. An already committed binding
avoids republishing in both cases. All cases recover the first sequence, preserve
input/token/body and binding identity, create one wakeup, and drain any duplicate
without replacing the first binding. Reference checks pass.

Captured normal/race runs each pass the four new cuts and all eight existing
mutation phase cuts, with 16 named PASS results each. The existing finite-window
transport is used unchanged; no new seed family/pin or production policy is added.
CI includes all four new controls in both modes. All 20 workflow Python heredocs
compile locally. No hosted CI completion is claimed.

## Evidence

Each helper uses a frozen 1164-file Go/module snapshot from `24af9b4` plus its
retained helper source. The first snapshot/binary/store is under
`/home/exedev/js-wf-native-signal-retained-recovery-20261011`; the second is under
`/home/exedev/js-wf-native-signal-expired-dedup-20261011`. `commands.json` records
actual build and probe process exits, timing and executable hashes. Every closed
copy file is bound by `closed-copy-census.json`; `copy-verified.json` records the
initial full-byte comparison. The verification commands that created the copies
are retained in the session tool history; no copy creator script was retained.

`model-run.py` captures the two actual component executables under
`/home/exedev/js-wf-signal-dedup-model-20261011`, with 1166 source inputs bound by
`model-source-inputs.json`. The initial uncaptured compilation failure and
corrected development test run remain in `model-initial-diagnostic.log`; they
are diagnostic only. `review.py` independently verifies original bytes, both
Git-resolved source snapshots/helper hashes, real process exits/binary hashes,
closed copy censuses and phase observations, model exits and each required PASS.
`review.json` records acceptance of that evidence, including the first helper's
expected failure.

The original native race remains failed. Full canonical scale, complete current
seed qualification, fault/soak/adoption gates remain open. Neither copied-state
recovery nor these component controls explain the native commit timeout.
