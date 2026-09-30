# Expanded release-count simulation at df72ed0

[Run 36782364759](https://github.com/AntPAllen/js-wf/actions/runs/36782364759)
completed successfully at clean `df72ed0cf0c94888d25ddaa1020f5c3471b67d14`.
Model version 3, 100,000 seeds per workload: 7,600,606 generated schedules,
169,856,712 choices and 1,881,853,898 transport events. Go test time was
6,214.969 seconds (1h43m34.969s). The complete CI log is retained; it includes
every workload result and the final coverage record.

This includes fan-out and first-heartbeat freshness workloads, but predates
raw blob metadata validation and all checkpoint foundation/capture changes.
The former has a separate clean 100,000-seed workload proof. Checkpoint capture
currently has local SDK controls and no seeded worker checkpoint workload.
This is the release seed count for these modeled slices, not final-source
full-coverage release completion and not closure of the mixed seed 65 miss.
