# Current-source full Tier3 matrix dispatch

Run [37156883771](https://github.com/AntPAllen/js-wf/actions/runs/37156883771)
is dispatched at exact `f05d7ba21442474767d3e204d601a486d7e6b054`.
This requests all 16 five-container R5 fault rows, each at seeds 1–200 and
the full ten-minute duration: 3,200 actual executions in 256 bounded shards,
at most 13 executions per shard and four parallel shards.

Rolling upgrades use SIGKILL and require a forced committed-before-dispatch
Start gap. Both server-clock rows require pending positive timer cuts and
five independent common-clock probes. Majority-route and no-quorum route rows
retain their distinct latency definitions. This is the full ten-minute matrix;
the separate Lame Duck profile, 24-hour full matrix and actual five-VM tier
remain independent requirements. The historical 30-second-TTL worker recovery
mismatch is not selected as a separate replay.

All four planner tests pass; an explicit enumeration confirms every row has
exactly seeds 1–200, without duplicates. These checks validate dispatch planning,
not executed workloads. The exact dispatch command, zero exit, head revision,
queued planner job `111301980761`, planned shard map and scope are retained.
All four archived files were reopened and SHA-verified before atomic rename.

Dispatch is **not accepted** and clears no runtime gate. Completion requires
successful actual row jobs and independent source-bound raw evidence review.
The current simulation and Tier2 jobs remain live; the older ahead-clock-only
campaign is also left running. No local 24h soak is launched with only 1.6 GiB
free on the root filesystem.
