# Fresh 200-seed campaign: seed 4 terminal latency miss

The fcf4537 campaign https://github.com/AntPAllen/js-wf/actions/runs/36835269534
passes seeds 1–3 and fails seed 4: mixedsignal/mixed-00-0 terminal p99 is
31.421256978 seconds from the last enabling event, above the unchanged 30-second
gate. The job stops there, so this is not 200 clean seeds. Independent pressure
and lease-disk jobs pass. The separate 844db83 20-seed campaign was clean; that
result does not erase this failure.

The full operation artifact has 400 events for this invocation. Its 52
pre-append renewals total 30.501003236 seconds, with 30.500405601 inside KV
Update and 0.000062393 at the local serialization gate. The logged slow window
contains 49 of those renewals (30.498502430 seconds), and 49 journal appends
sum 0.277316472 seconds. Other workers' failed acquisitions and heartbeat gate
waits overlap these windows; summing all operation durations is not elapsed
critical-path attribution. These are client call durations, not measured server
execution or proof of a NATS bug. Server cause remains unconfirmed.

Full seed-4 operations, dispatch, fault schedule, store metadata snapshots and
compressed server/disk logs are retained, with exact nanosecond renewal samples
for a future trace-cost Tier 1 reproduction. The earlier uniform lease-cost
model already proves that expensive mandatory updates can exceed the latency
gate; it does not reproduce the observed per-request sequence or its cause.
No renewal, fencing, recovery gate or scanner behavior is changed here.
