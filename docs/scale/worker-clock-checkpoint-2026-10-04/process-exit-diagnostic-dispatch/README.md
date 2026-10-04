# Focused primary-error diagnostic dispatch

[Run 37170797762](https://github.com/AntPAllen/js-wf/actions/runs/37170797762)
is dispatched at exact **19f3cb36c148b95f94737400dbaa158247814fd6** for
worker-clock seed **6**, full **10m** duration, one requested case. The API
confirms that source and queued planner **111343180483**. Exact command,
workflow and initial API metadata are preserved. Dispatch/queue state is not
execution, recovery, qualification or proof of the previous failure's cause.

The prior raw failure shows all five child processes die on clock-proof
context deadlines before the parent reports stale samples. The new source
observes those actual exits promptly and identifies publish versus broker-read
errors. Its actual-child controls already pass normal/race. This run asks
whether that failure recurs and, if it does, which probe phase fails. All
probe/deadline/freshness and workload gates remain unchanged. Original failed
full parent 37164231641 is not replaced or promoted; other live jobs continue.

Provisioning creates workflow streams/buckets but does not create the 64
partition consumers. Each of five children concurrently binds all 64 partitions,
so consumer creation and clock publication overlap during startup. The original
server logs show consumer election/quorum churn in that period. This is a
startup-pressure hypothesis, not an established server cause or implemented
startup fix. Do not turn it into qualification without actual evidence.
