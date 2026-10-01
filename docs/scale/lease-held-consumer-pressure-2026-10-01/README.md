# Held acquisition traffic alongside lease and consumer pressure

This extends the paired four-row fixture to six rows: healthy/delayed disk ×
eight owners alone, owners with consumer traffic, and owners with traffic plus
held acquisitions. Each owner still performs 48 unconditional renewals and 48
CAS appends. Every fourth append performs production lease.Acquire against that
active owner with a different worker ID, requiring ErrHeld and no returned lease.
This supplies 12 probes per owner, 96 per contender row. All existing ownership,
epoch/revision, journal integrity, fixed leader placement and physical consumer
traffic checks remain required.

Probes use a dedicated connection pinned to healthy node 0, with reconnect and
server discovery disabled. Its outgoing-message delta must be at least 192
(two requests per probe); the actual pinned client sends 288. Initialization
occurs before measurement, and this connection carries no foreground/background
load. This guards against counting successful local rejection as network load.

The R3 lease/state and consumer leaders remain on healthy node 0; the journal
leader stays on node 1, with node 2 stopped. Delayed rows inject 70-ms disk
latency on node 1. Each busy row requires 384 actual stream sequences, all eight
consumer acknowledgement floors advancing by 48, exact payload identity, one
delivery, full drain and unchanged placement. Owners are checked/released before
waiting for the background tail. No TTL, renewal, latency target or runtime
policy changes are made.

## Final results

The final six-row race passes in 139.003 seconds (test body 137.98 seconds).
Final go vet ./integration and git diff --check pass.

| Disk | Consumer traffic | Held probes | Probe requests | Per-owner summed KV Update seconds |
| --- | --- | --- | --- | --- |
| Healthy | No | 0 | 0 | 0.039767–0.045635 |
| Healthy | Yes | 0 | 0 | 0.064705–0.082041 |
| Healthy | Yes | 96 | 288 | 0.062698–0.069672 |
| Delayed | No | 0 | 0 | 8.142801–9.776521 |
| Delayed | Yes | 0 | 0 | 7.667786–8.424252 |
| Delayed | Yes | 96 | 288 | 10.229843–11.356875 |

Delayed contender rows remain below the real seed-4 invocation's 30.500405601
seconds of KV Update time. A single sequential paired run cannot establish
monotonic load causation or rule out other contention shapes. The probes are
paced once per four appends across eight keys through a healthy client; this
is not the real invocation's single-key contender concentration, asymmetric
worker transport, heartbeat contention, retry/redelivery, leader election or
replica catch-up. The real latency failure and server cause remain open.

## Semantic negative control

A compiled Go overlay makes production Acquire return ErrHeld immediately for
worker IDs starting with contender-, without any transport calls. Foreground
operations and local held-call counts continue, but the dedicated connection
reports zero requests. The test fails in 7.546 seconds on:

    held probe network messages=0 want at least 192

The retained patch/log and overlay source hash identify this control; it is not
promoted. This is a semantic assertion failure, not a build error or timeout.

## Hosted scope and reproduction

Prior main fa99d53's hosted tier2-mixed run 36841144769 completed successfully:
mixed recovery, original lease-append pressure, lease-disk contract and the
four-row consumer-pressure job all pass. Its jobs metadata and four-row raw
artifacts are retained here. Those results predate these six rows; hosted
confirmation for the new probe assertion remains pending.

    WF_LEASE_CONSUMER_PRESSURE=1 LEASE_APPEND_REPORT=/tmp/lease-held-pressure go test -race ./integration -run '^TestLeaseAppendPressureConsumerTrafficFixedPlacement$' -count=1 -timeout=6m -v

This fixture is test-only. The full latest-model simulation, whole-matrix
200-clean-seed acceptance, five-node 24-hour soak, online GC and remaining
capacity/combined-fault gates remain independent and open.

## First hosted six-row observations

The 463f648 push run 36843492527 fails its pressure job in 22.928 seconds.
All three healthy rows pass; in the first delayed row (before consumer load
or contender probes), three owners stop at 16/17 appends with ErrUnknown:
tail lookup returns API 503/10008, JetStream temporarily unavailable.
Other owners complete all 48 calls. Raw rows, final node metadata, job log
and compressed disk trace are retained. This is not a held-probe assertion
failure and does not identify a server cause. No runtime or fixture gate is
relaxed to turn it green.

The independently dispatched 200-seed run 36843533447's six-row pressure job
passes on the same source. Its mixed campaign remains live; neither result
is the whole sustained matrix release gate. Both observations must remain
visible rather than treating the independent pass as erasing the failure.
