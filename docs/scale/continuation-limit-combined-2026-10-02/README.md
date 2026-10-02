# Continuation budget across worker kill and cluster restart

Three actual race-instrumented cuts pass at a private 20-entry test budget:
after SignalConsumed, after StepCompleted, and after Failed. Each uses a real
worker subprocess SIGKILL while its production 12-second lease remains held,
then shuts down all three NATS servers and restarts their retained file stores.
Server shutdown here is graceful; only the worker receives OS SIGKILL.

Replacement consumes the original unacknowledged dispatch and restores the
checkpoint frame without archive reads or prefix handler reexecution. The
journal remains exactly 20 entries with its original prefix and one terminal
ErrTooLong, matching immutable state on every peer. No forbidden effect runs.
Offline replay follows both continuations, consumes all recorded SDK steps,
and stops at the retained limit request without executing that effect.

The compiled negative control replaces only the three absolute-index budget
checks with restored-suffix-length checks. The actual after_signal test fails
with `limit outcome changed: <nil>`: its journal has 22 entries, ends Completed,
and the forbidden effect executes exactly once. This proves semantic detection,
not merely a failed compiler or timeout.

`originals.tar.gz` retains all 55 non-store files from the final runner, including
raw journals, cut prefixes, lease/frame evidence, Go JSON events, exact mutant,
source hashes, runner result, and independent review. Every member was compared
by SHA256 after compression; `manifest.json` records the hashes. Original stores
remain at `/tmp/js-wf-continuation-limit-combined-contract-20261002`.
Worker vet passes. The final source hashes match the pending implementation.
Earlier fixture failures and original stores remain in their temporary roots.

This closes the local combined contract at budget20 only. The opt-in workflow
also accepts the production default100000 without overriding the worker cap;
that qualification is still required. This does not clear the full matrix,
24-hour runtime soak, or Tier1 modeling of these combined cut boundaries.
