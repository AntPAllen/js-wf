# Bound journal compaction checkpoints and renewal recovery

`CheckpointCompaction.Checkpoint` now emits canonical portable staging input.
`ResumeCheckpointCompaction` requires the caller's expected workflow type/ID,
runtime checkpoint and logical tail, then binds the opaque stage to fresh
canonical source authority, exact archive cut/application and configured payload
limit. Source-head changes invalidate the descriptor. No reader is acquired on
resume: that would change the captured head. The canonical source root protects
its forests; source changes reject further work or original-head publication.

Verification progress remains private. Resuming a descriptor saved during final
verification restarts the entire verification, without trusting a completed
prefix certificate. The envelope schema is
`js-wf-journal-compaction-stage-v1`, capped at536576 bytes. Underlying root/fence
and graph staging schemas are unchanged. Duplicate, alias, escaped, unknown or
noncanonical envelope encodings are rejected.

`BeginIntentRenewal` exposes the staging renewal phase to journal callers.
Checkpoint emission during renewal retains the **pre-renewal stage input and
exact requested expiry**, including after unknown updates. Save that envelope
before advancing renewal. A fresh resume reconciles matching old/new grant
expiries before the old deadline; failures remain sticky on the old handle.
Renewal examines at most the caller's node-budget number of scopes per call,
with the underlying namespace enumeration/context limits. Completion returns to
staging; no journal root is published by renewal.

## Development evidence

Thirty-four JSON/protobuf controls cover ordinary and per-stage-batch resumption,
renewal and dropped/lost updates, restart of verification, wrong identity/tail,
application/cut/payload-limit substitution, append, reader, retirement and
collector interference. Eleven malformed envelope controls preserve authority.
Healthy12-record relocation uses10 verification batches; renewal uses11 scope
batches. Wrong binding bypasses fail6 controls, identity bypasses8, requested-
expiry omission6 and canonical-envelope bypass5. Exact sources/outputs are
retained. The preliminary retirement fixture attempted to retire a suspended
workflow; its2 failures are retained/excluded, and the corrected fixture first
appends terminal completion.

Native R1/R3 domain scenarios retain an old reader while saving pending renewal
input and restarting all embedded peers. Fresh store/adapters resume the bound
input, complete10 renewal batches and8 independent verification batches, and
sweep past the original intent expiry. Each then restarts all peers again after
archive publication. Both verify preserved reader authority/bytes, successive
compactions, logical audit, original receipt collection and zero physical chunks
after retirement. The saved envelope remains in the client process; this is no
claim of runtime descriptor-store durability, process SIGKILL or VM/storage loss.
Native2-minute parent contexts are unchanged. Clock advance is controlled through
the shared journal/collector clock, not a real VM clock-jump test.

The broader journal checkpoint/archive race suite, restored45-control selection,
all22 worker maintenance/seven handoff repairs and all853 unchanged common saved
traces pass. Run
`python3 docs/scale/graph-journal-compaction-checkpoint-2026-10-10/review.py`
to verify fixed inputs and retained component evidence. Earlier evidence remains
frozen at its own commits.

## Remaining work

The journal provides bound resumable input and explicit staging renewal. Worker
selection/persistence of that input, lease checks, durable descriptor/expiry
storage, recovery of interrupted maintenance, verification lifetime renewal and
bounded-memory namespace enumeration remain required. Reader/head interference
still requires a fresh plan. The worker still uses its15-second whole handoff
deadline and fixed intent expiry, with no persisted maintenance progress.
Actual100000 and all original full/extended/native/fault/scale/soak/retention/
import/admission/collector/rollout gates remain open. Public graph continuation
admission stays closed; production collection stays off.
