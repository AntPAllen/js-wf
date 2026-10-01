# Retirement replay guard follows durable publication — October 1, 2026

CI test run 36805207254 at 3a5ed7e failed the retirement contract because the
reused workflow returned result 2 with four initial-handler calls and three
effects. The former assertion required exactly three calls across all three
invocations/generations. The log does not identify what caused the extra call.
The continuation contract already permits initial replay between committed
checkpoint completion and runtime-manifest publication.

The revised real R3 contract adds a second case that drops exactly one fresh
manifest Create before commit. The retry legitimately enters the initial handler
again and repairs publication from the recorded checkpoint. It produces the
same four-calls/three-effects shape without repeating an effect. This establishes
that fixed entry counts do not distinguish permitted repair from a bad fast
resume; it does not establish that this precise fault occurred in CI.

The actual resume invariant is now enforced directly: each fresh initial entry
reads the same generation's checkpoint manifest and must find it absent. An
entry after publication fails the workflow. Fresh-entry counts are separately
recorded; the earlier two invocations, exactly three total effects, stale-frame
rejection, immutable results, survivor references and collection assertions
remain enforced. The controlled loss must be consumed exactly once and require
at least two fresh entries.

## Evidence

- `real-race.log`: both cases passed in 39.228 s. Baseline had three calls,
  one fresh entry and no injected loss. Controlled loss had four calls, two fresh
  entries and one drop. Each had three effects, two current terminals,
  generations 1 and 3, two old objects reclaimed and the shared blob retained.
- `ignore-frame-mutation.log`: compiled production overlay ignores a published
  frame only for invocation generation 3. The baseline contract fails on
  initial entry after manifest publication in 18.561 s, proving the replacement
  assertion still catches loss of the fast resume contract.
- `ci-3a5-failure.log`: retained original clean-runner failure and job command.
- `vet.log`: integration/sim vet passed without diagnostics.
- Source hashes bind the final fixture and runtime dependencies.

No runtime code changes are made for this correction. Fresh CI validation is
still pending. Combined retirement faults, broader continuation gates,
final-source full matrix/soak, online GC and mixed seed 65 latency stay open.
