# Actual 24-hour journal-leader row started

Executed source `20babb566534e3ea7aa06b5526a748c9d65c6ed5`, seed1, race,
24h duration /24h20m test deadline. Five actual containers run the existing
journal-leader row with the original mixed workloads,30s fault cadence,
checkpoint/integrity/drain/history and terminal/progress gates. Producer:
`scripts/run-tier3-soak.py`, isolated checkout and actual executable retained at
`/tmp/js-wf-soak-journal-24h-20261004`.

Persistent unit:`js-wf-soak-journal-24h-20261004.service`.
Supervisor15697 /test launcher16037; launch observation records service invocation,
boot ID, process start ticks, actual argv, executable SHA/build info and exact
source/command/environment snapshots. These are live launch snapshots, not final
archives or passing evidence. Current named test has started; outcome and source
post-check/retained-original audits remain pending. Observe the existing service
and processes before deciding it is stopped; never restart on observation timeout.

The VM has about44GiB free at launch. Earlier rough13.35GB/day extrapolation is
not a steady-state guarantee. Monitor actual root/store growth and retain failed
originals. Other matrix rows remain required; this single row cannot qualify
full Tier3 or the complete 24-hour matrix.

Observe:

```sh
systemctl --user show js-wf-soak-journal-24h-20261004 --property=ActiveState --property=SubState --property=MainPID
cat /tmp/js-wf-soak-journal-24h-20261004/execution.json
tail /tmp/js-wf-soak-journal-24h-20261004/events.jsonl
```
