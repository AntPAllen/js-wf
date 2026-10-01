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
