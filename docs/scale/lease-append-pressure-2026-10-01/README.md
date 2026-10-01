# Fixed-placement lease renewal and journal append pressure

Base main: 1db0bcf542eddbfd0b30deb5287b015ca438b258. Production acquisition,
renewal, writer fencing, backoff and all latency gates are unchanged.

## Motivation and controlled scope

Retained hosted mixed seed 9 snapshots at dd7389b report healthy node 0 as lease
leader during recovery, delayed node 1 as journal leader, and lagging node 2.
Replacement signal workflows spend 30.573/29.973 seconds in 43/48 KV updates,
with 69.631/76.973 microseconds local gate wait and zero errors. Snapshot
placement is evidence at its timestamp, not per-RPC attribution.

The new leaf fixes KV_WF_LEASE leader on node 0 and WF_JRN leader on node 1,
waits for full catch-up, warms both data/Raft file sets, then stops node 2.
Pinned node-0 clients execute 48 unconditional production RenewTimed(minIdle=0)
calls per owner, either alone or followed by production journal CAS Append.
Rows run with no disk delay then 70 ms delay on node 1's existing store files.
One/two owners retain separate keys/subjects. Leaders are checked after each row;
there is no per-RPC leader history. The third node stays stopped and never
participates in catch-up, unlike the hosted mixed run.

The row protocol synthesizes 48 journal records for direct method measurement:
Started, 23 request/completion pairs and Completed. No invocation start,
workflow handler, run consumer, terminal KV publisher or signal pipeline runs.
This is not a successful mixed workflow or a full raw-state audit. Reads check
all 48 records' indices, epochs, worker identities and terminal kind. Renewal
acknowledgments must advance KV revision without changing owner/epoch. Individual
renewal/append requests are bounded at three seconds. The delayed renewal-only
row must meet the 48×70 ms lower bound; the retained trace must show DELAYED.

## Final race results

| Disk delay | Appends | Owners | Row duration | Renewal totals per owner | Journal append totals per owner |
| --- | --- | --- | --- | --- | --- |
| none | no | 1 | 0.015 s | 0.015 s | 0.000 s |
| none | yes | 1 | 0.065 s | 0.020 s | 0.045 s |
| none | yes | 2 | 0.075 s | 0.022 s, 0.021 s | 0.052 s, 0.054 s |
| 70 ms | no | 1 | 3.462 s | 3.462 s | 0.000 s |
| 70 ms | yes | 1 | 6.970 s | 3.454 s | 3.516 s |
| 70 ms | yes | 2 | 10.847 s | 4.416 s, 4.336 s | 6.431 s, 6.510 s |

The delayed two-owner row measures KV update totals 4.410/4.332 seconds and
local gate waits 0.499/0.458 ms. Maximum individual renewal is 143/209 ms.
These operation totals do not identify internal server processing time. The
stable two-replica pressure contract does not reproduce the hosted ~30-second
KV totals. Recovery transients, third-replica catch-up or load from other groups
are still unmeasured differences; no NATS bug or causal explanation is claimed.

The final contract passed under race in 31.533 seconds. An earlier version
without inner KV/gate timing passed in 28.629 seconds with similar durations.
A compiled skipped-renew control returns claimed timing/ack success but does
not touch KV; it fails the actual revision-advance assertion in 5.117 seconds.
This is a compiled semantic failure, not a timeout. Vet, whitespace checks and
workflow YAML parsing pass. The new lease-append-pressure job runs independently
beside mixed chaos on pushes, manual triggers and the existing daily schedule,
with required artifact upload. Hosted results remain pending.

```sh
WF_LEASE_APPEND_PRESSURE=1 LEASE_APPEND_REPORT=/absolute/path/report go test -race ./integration -run '^TestLeaseAppendPressureFixedPlacement$' -count=1 -timeout=5m -v
go vet ./integration
```

The writer's per-append renewal is not cached or skipped. The five-second held
lease redelivery delay is unchanged. The existing rejected-create leaf proves
no WAL growth for initialized held-key rejections in a stable cluster; neither
leaf rules out load or behavior during recovery. The next matching control
should add replica catch-up and run/signal group traffic before considering an
optimization. Mixed p99 misses, full 200-seed matrix, five-node 24-hour soak,
online GC and remaining plan requirements stay open. Original full simulation
campaigns and million timers were not restarted or canceled.
