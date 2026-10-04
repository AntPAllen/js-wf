# Armed retained-audit diagnostic launched

Pinned source `18ddf0454de13a2d5df2c1a8b22efc5f765f2de0`, journal/seed1/race,
requested24h. Existing24h20m test timeout and audit20s attempts /60s total remain.
`--retained-audit-trace` records each checkpoint attempt's delegated stream/KV
read counts, bytes, elapsed/max durations, errors and latest64 completed calls.
It forwards request contexts/options/results/errors unchanged. It is a method
trace, not exhaustive wire/CPU/object-store instrumentation or a cause diagnosis.

Persistent service `js-wf-soak-journal-audit-trace-20261004.service`, supervisor
29878 /test launcher30172. Actual isolated source/SDK/raw/store root:
`/tmp/js-wf-soak-journal-audit-trace-20261004`. The old failed attempt is preserved
separately. This is one instrumented execution, with no gate relaxation or
qualification claim. Observe the existing service and actual process before
considering any restart; observation timeout is not a terminal result.

These metadata files are live launch snapshots. Final outcome, source post-checks,
raw method trace interpretation and original-store audits remain pending.
