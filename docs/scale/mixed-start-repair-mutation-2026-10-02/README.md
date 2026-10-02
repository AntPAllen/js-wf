# Mixed production start repair mutation

The shared real-cluster fixture admits four pending shorts, three suspended
timer parents, two suspended signal parents and a suspended fan-out parent
with six children and twelve grandchildren. After joining the initial workers,
it runs production Start for one extra short invocation through the existing
dispatch-dropping JetStream adapter. The real invocation publish commits;
all run publication attempts fail, and Start returns ErrEnqueueUnknown with
the retained invocation sequence. No committed record is deleted to create
the gap. Raw journal, state and dispatch reads verify the orphan is unstarted.

The fixture SIGKILLs the observed journal leader, verifies the exit signal and
restarts its retained store. On a surviving node it verifies the gap again,
reads the original invocation by sequence, then calls production StartScan
with that sequence and a one-record budget. The intact scanner inspects and
reenqueues one record; the actual retained dispatch body and stable generation
message ID are logged. Replacement workers complete the original28 plus the
orphan, and raw-state integrity proves29 invocations/journals/terminals.

The source mutant returns immediately from StartScan. The fixture observes
zero inspected records and zero retained dispatches. Replacement workers still
complete and audit the original28, preserving their journal prefixes and higher
fencing epochs. The retained29th invocation remains without a journal, state or
dispatch. That exact liveness failure triggers the semantic escape; the test
does not wait for an Await timeout or manually complete the orphan.

The final runner passes its baseline in46.094s and detects the mutant in47.087s
(including build/runner overhead). The baseline race run passes in36.64s with
all29 terminals and no race warning. Compilation and unrelated-failure controls
are rejected. Independent checking decodes the invocation input/hash, binds its
sequence to admission, verifies the actual dispatch body/message ID, and checks
named pass/fail outcomes and source hashes. Original JSON logs/reports are
losslessly archived with verified member SHA256 manifests; race events are
losslessly compressed. The final fixture is based on the `6d958d5` worktree
before commit.

The first baseline reached its150-second deadline during a post-fault read;
the runner rejected it without running or claiming a detected mutant. Its
originals are preserved separately. The final fixture bounds each verification
attempt to3 seconds within a30-second readiness window. Two orphan journal-read
attempts expire, then the baseline recovers. The failed run does not isolate
which journal-opening/read request stalled or establish a server-side cause.
Neither the runtime recovery target nor the fixture's150-second total deadline
was increased.

Hosted run37007279855 at `4e7de02` is independently accepted: all six jobs
succeed, eleven baseline/mutant pairs execute with named pass/fail outcomes,
recorded production/fixture hashes match the Git revision, and both negative
controls per job are rejected. Actual raw orphan receipts bind the retained
input/hash to the generation-tagged repaired dispatch; the mutant retains its
invocation without a dispatch. Original artifacts, full job logs and terminal
metadata are losslessly archived in [ci-pass](ci-pass/), with independent
acceptance and member SHA256 records.

This advances the fifth mixed source-mutation category. Purge now has separate
local evidence; its hosted acceptance and the full mixed-chaos release campaign
remain open.

## Subsequent fixture readiness failure and bounded evidence requests

Run37008116036 at `82494c5` passes the baseline but rejects the start-repair
mutant: after two short journal-read retries it reaches the150-second total
deadline before emitting the invocation receipt. The remaining WF_INV metadata
and evidence requests still used the whole fixture context. The artifact does
not identify which request stalled or establish a server-side cause. The whole
seven-job campaign is failed; the separately successful purge component is
recorded with that limitation and all failed originals in
[purge campaign evidence](../mixed-purge-mutation-2026-10-02/ci-purge-pass-campaign-failed/).

The fixture now bounds every challenge request to2 seconds with at most three
attempts and labels any exhausted request. This includes invocation/run metadata,
source/dispatch reads, before/after snapshots and the scan. Retried repair uses
the same invocation sequence and generation-scoped message ID. An availability
failure cannot produce the semantic escape; the total150-second fixture deadline
and all cohort/integrity requirements remain unchanged.

The final pair passes the baseline in36.09s and detects the mutant in36.57s
test time. The race baseline passes in36.71s (37.735s package), with all29
terminals and no race warning. Independent source/receipt/outcome checks and
losslessly archived originals are in [all-requests-bounded](all-requests-bounded/).
This fixture is based on the `e86165a` worktree before commit; hosted acceptance
of these additional bounds remains pending.
