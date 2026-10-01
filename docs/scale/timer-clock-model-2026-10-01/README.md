# Deterministic timer clock-source transition model

`TimerScheduleTransport` now supports an explicit leader wall-clock offset.
`LeaderNow` can supply timer origin timestamps; native absolute deadlines are
compared against the current leader clock. Delivery callback timestamps,
quorum healing, retry budgets and message-ID dedup stay on controller virtual
elapsed time. Existing workloads retain the default zero offset.

This is a transport model assumption, not a verified NATS scheduling contract.
The workload does not execute production worker decisions and cannot certify
production clock-skew safety or explain a real-cluster failure by itself.

The seeded workload covers four combinations: initial clock +60s or -60s,
with transition before the original two-second due time or after that due time
while quorum is unavailable. It verifies no delivery without quorum, no duplicate
delivery after transition, exact controller receipt times and exact trace replay.
With +60s origin, an absolute deadline reaches controller time +62s after the
transition. With -60s origin, the retained deadline is already past on the new
leader and fires when quorum heals. These outcomes characterize sensitivity;
they are not passes against the plan's under-30-second timer recovery contract.

Four first-covering traces are retained in `sim/testdata/regressions/` and wired
into the general replay/minimization dispatcher. The 1,000-seed focused workload
passes in0.056s. A combined timer-model regression check (new transitions,
existing timer publication and production-worker burst workloads) passes normally
in2.130s. Comprehensive release simulation and real R5 source-transition
acceptance remain open; the active comprehensive run predates this addition.

The combined race check passes in37.235s with1,000 seeded cases per selected
workload and the complete pinned regression corpus. A separate explicit run
verifies all four new disk traces through `TestPinnedRegressionCorpus` (0.007s).
The initial pin-generation command requested100 seeds and was rejected by the
existing1,000-seed minimum; the corrected1,000-seed command generated the pins.
No rejected command is counted as passing evidence.

## Production-worker burst characterization

The existing eight-timer production-worker workload now also runs with a shifted
`TimerNow` origin and an unshifted scheduling leader after the worker has durably
suspended. It uses the real worker, SDK timer handles, lease acquisition, journal
appends and outcome persistence against the modeled transport ports. Both
variants retain35 ordered entries, one terminal result42, two handler calls,
all eight timer sources and a passing retained-state invariant audit.

Across1,000 seeds, the ahead-origin variant completes at virtual controller
+62,000ms and the behind-origin variant at+0ms. The seeded test deliberately
asserts these counterexample outcomes: a green characterization test is not a
pass against the desired timer timing. Two pinned traces retain these cases,
raising the corpus to174. The general replay dispatcher now supports both the
new worker-clock workload and the existing worker timer-burst workload.

The focused production-worker characterization passes in1.778s. This narrows
the issue to production decisions under an explicit transport clock contract:
the SDK retains `FireAt = TimerNow + duration`, and resumed timer readiness
compares wakeup timestamps with that retained absolute deadline. Whether NATS
actually exhibits this contract during a leader change still needs admitted
in-flight timer cuts and real retained evidence. No production behavior was
changed in response to the model alone.

The combined race check passes in62.063s with1,000 seeds for the new
production-worker characterization, existing worker burst and transport clock
workloads, plus all174 pinned traces through the general replay dispatcher.
