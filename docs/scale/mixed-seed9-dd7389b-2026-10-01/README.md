# Hosted mixed seed 9 latency miss at dd7389b

[Run 36814833995](https://github.com/AntPAllen/js-wf/actions/runs/36814833995)
passed seeds 1–8 and failed seed 9 at terminal p99 34.31501411 seconds across
all 28 outcomes. It failed before final integrity/drain checks, so it neither
exercises nor clears the bounded final lookup fix. Source:
dd7389bf646fa134edae6e73ab9a8dcd9bb567fc.

Fault placement: 70 ms disk delay node 1, isolate/kill node 2, pause node 0
at 12 ms, kill at 44 ms; the fixture removes quorum for 12 seconds before heal.
The operation artifact records replacement signal mixed-02-0's 43 pre-append
renewals taking 30.574 seconds total, including 30.573 seconds KV update and
69.631 microseconds local gate wait. mixed-05-0's 48 renewals take 29.973 seconds,
including 29.973 seconds KV update and 76.973 microseconds local gate wait.
Both groups have zero errors. These are measured operation sums, not isolated
server processing times or causal proof. Raw schedule, operation events, disk
trace and monitoring snapshots are retained. No renewal/fencing/latency gate
has been weakened. The server cause remains unconfirmed.

The earlier c56bda5 mixed campaign separately passed all 20 seeds and its
held-lease disk contract in run 36813976941. That green campaign does not erase
this miss or establish the 200-seed full-matrix release gate.
