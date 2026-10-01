# Continuation definitions in execution and replay plugins

worker.WorkflowDefinition exports initial Handler and named Continuations. The
worker runner accepts maps/factories of definitions, validates registrations and
passes WithContinuations into construction. The replay CLI accepts a single
exported definition/factory and uses ReplayWithContinuations. Existing function
and handler-map plugins remain supported.

The shared fixture persists state 23, passes original input 7 through two named
checkpoints, suspends on a signal and executes one effect returning 60. The
worker runner test exercises definition-map and factory exports on real R1
stores. The CLI test executes the fixture on real R3 stores, exports the complete
journal and two checkpoint objects, audits suspension/completion, runs a factory
export and replays a saved bundle against an unreachable NATS URL. Raw integrity
passes; replay does not increment the shared effect counter. Changed stage
requests, missing registration and missing checkpoint objects are rejected.

Synthetic cancellation and rejected-effect Failed tails reuse the real
checkpoint prefix to exercise the specialized CLI replay validators. These are
not additional real cancellation/journal-cap durability proofs. Existing real
continuation/cancellation/cap evidence remains independent.

Full command package race suites passed: wf 23.412 seconds, wf-worker 6.651
seconds. A compiled execution mutant omitting the continuation worker options
fails semantically with a terminal "continuation support is not configured".
A compiled replay mutant dispatching only the initial function fails the
recorded stage suspension check (2/9 steps). Vet/diff checks pass.

Two setup diagnostics are retained. An unbounded R3 provisioning call exhausted
45 seconds before handlers ran; bounded three-second idempotent provisioning
attempts recovered in the later runs. The first recovered test also identified
that the CLI replaced missing-object errors with a step-count mismatch; the
production error path now preserves missing objects, corrupt journal and
unknown continuation errors. No server cause is inferred from provisioning.

This closes basic plugin registration and continuation offline CLI dispatch.
Full fault matrices, combined publication/retirement cuts, non-step public replay
metadata acceptance and release gates remain open. No scheduler or simulator
transport changes are made, so pinned model traces remain unchanged.
