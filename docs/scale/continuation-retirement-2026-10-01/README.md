# Continuation retirement, quiescent GC and ID reuse — October 1, 2026

A real R3 three-node worker contract completes two checkpointed invocations.
Each has its own identity-bound frame/archive, and both archives reference the
same content-addressed 921,602-byte step result. All worker loops are stopped
before every blob sweep: this is quiescent collection, not online GC.

## Lifecycle verified

1. The first sweep preserves both live frames, both archives and the shared
   result. The workers executed two initial handlers/effects.
2. Retiring one terminal invocation removes its runtime manifest and produces
   ErrPurged. ReadCheckpoint finds no remaining pointer for that generation.
3. A second sweep reclaims exactly its old frame and archive (two objects),
   while preserving the other invocation's frame/archive and shared blob.
   Shared content bytes match the original payload before any fresh invocation
   can rewrite the same object and conceal a collection error.
4. Starting the same ID produces invocation generation 3 after generation 1.
   Injecting the saved old manifest is rejected with ErrCheckpointGeneration by
   both the reader and a real worker, before another initial handler/effect.
   Its old frame/archive objects have already been collected; rejection happens
   before accessing them. The injected manifest is removed with revision CAS.
5. A fresh worker delivery then completes result 2 with newly materialized state
   and a generation-bound frame distinct from its predecessor. The surviving
   invocation still returns result 1. Cross-peer reads and raw-state integrity
   prove two current terminal invocations. Exactly three initial handlers and
   effects ran across three real generations/invocations.
6. Final quiescent collection preserves the fresh and survivor frames and the
   shared result; its full content bytes still match the original payload.

The shared result is referenced from the survivor's archival journal, not a
frame-held promise. Frame-held promise retention is covered by the separate
checkpoint-manifest contract; its complete retirement/model gates remain open.

## Evidence and limits

- `real-race.log`: final R3 contract passed under race in 19.385 s (18.34 s test
  time), with two reclaimed objects, generations 1 and 3 and two current terminals.
- `manifest-mutation.log`: compiled production overlay skips snapshot deletion
  during retirement. The real contract fails because the retired manifest remains
  in 3.420 s. This is not a compile failure; no overlay alters the worktree.
- `vet.log`: vet exited zero with no diagnostics.
- Source hashes bind the test and runtime dependencies; runtime code is unchanged
  from parent 5696347. This commit adds a contract and documentation.

Retirement process-crash/reply-loss combinations, seeded integrated continuation
retirement/GC, reusable promise/cancellation/timer/panic/entry-limit gates and
independent final-source matrix/24-hour soak remain open. Online GC is unsupported.
The mixed seed 65 latency failure is unchanged.
