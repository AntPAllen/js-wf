# First-heartbeat freshness controls

The worker now applies its acknowledged-renewal freshness bound to every
heartbeat, including the first. Acquisition and journal append renewal already
provide a successful lease write. The freshness interval starts before that
write, not when its acknowledgement arrives. An overdue heartbeat must still
update; every journal append still updates unconditionally.

- `before.log`: the strengthened integrated model rejects the previous worker
  at seed 1 because it makes an unnecessary first-heartbeat KV update.
- `100k.log`: the lease reuse, integrated heartbeat and failure-handoff workloads
  pass 100,000 seeds each: 300,000 schedules and 10,247,713 transport events.
- `real-race.log`: real three-node recent/overdue first-heartbeat and lease
  contracts pass, including four unconditional journal append renewals.
- `full-sim.log`: all modeled workloads and pins pass in 84.35 seconds.
- `mixed12-race.log`: local replay of CI failure seed 12 passes at 17.53 seconds
  terminal p99 across 28 invocations. Real interleavings differ, so this is not
  causal attribution or full-matrix release acceptance.

These local logs were produced with the pending policy change on base `97e72f4`.
The commit containing this directory contains the corresponding source changes.
The earlier failed CI artifact remains separately retained.
