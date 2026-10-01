# Original-terminal journal-limit replay

Production Replay and ReplayWithContinuations now validate retained
Failed.LimitRequest directly. The raw terminal and anchor array remain intact;
only the SDK entries receive the pending rejected request at the terminal index.
Invocation identity is required. CLI, real R3 continuation-limit and seeded
continuation-limit fixtures use the original failure history.

Evidence retained here:
- Full wf and CLI race suites: 12.574 / 21.038 seconds.
- Real three-replica modeled-budget continuation contract: 18.506 seconds under
  race, two continuations, no rejected effect and no archived-prefix reads.
- 1,000-seed limit workload plus pinned corpus under race: 33.481 seconds. Exact
  first-ten schedule replay and separate-process seed 42 remain in the workload.
- Standalone pinned corpus: 0.238 seconds; old pins unchanged.
- Compiled baseline overlay: semantic failure because the original Failed
  history reaches the journal tail instead of auditing the rejected request.
- Compiled callback-execution overlay: semantic failure, effects=1 even though
  the pending-step error remains correct.
- Final package vet and diff checks pass.

New unit cases cover identity required, wrong generation/error, null/empty/array
requests, result/LimitEntry ambiguity, changed declaration, hidden pending SDK
entry and an unknown rejected continuation stage before handler code. A full
100,000-record synthetic journal separately verifies no raw append or cap
inflation is required; it is not a real server durability proof. Existing real
production-limit proof remains independent.

The in-memory workload retains the same transport decisions, so regression pins
are not regenerated. No worker/transport scheduling behavior changes. Non-step
LimitEntry validation is still CLI-specific. Continuation plugin registration,
combined real process/server cuts near the cap and full release gates remain
open. Hosted simulation campaigns started on older commits do not prove this
new replay source.

Final-source 100,000-seed continuation-limit workload passed in 136.405 seconds.
The added hard-cap and metadata unit cases passed under race in 6.258 seconds.
An earlier pre-alternation-validation compile passed 100,000 seeds in 158.780
seconds; only the final-source log is retained as the authoritative count proof.
