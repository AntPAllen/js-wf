# Paired lease pressure with replicated consumer traffic

This extends the fixed-placement contract with a separate opt-in four-row
comparison: eight independent foreground owners, with/without consumer traffic,
on healthy and delayed disk. Foreground owners retain the same 48 unconditional
renewals and CAS appends each. KV lease/state leaders stay on healthy node 0,
journal leader on node 1, node 2 stopped; delayed rows inject 70 ms on node 1.
Existing one/two/eight-owner rows remain in their original test.

A dedicated R3 WorkQueue stream and eight explicit-ack durable consumers share
the same physical stores. Stream and consumer leaders are pinned to healthy
node 0. Warm publishes/deliveries/DoubleAck occur before SlowDisk attachment.
Traffic rows publish and confirm exactly 384 payloads, checking payload identity
and one delivery each. The raw stream LastSeq grows by 384 and every durable
acknowledgement floor grows by 48. Stream messages, pending and ack-pending must
all drain to zero; leaders are rechecked. These are synthetic transport load,
not real workflow invocations or a replay of mixed fault/recovery traffic.

The first fixture waited for all background traffic before checking foreground
lease ownership; completed owners then expired at the unchanged twelve-second
TTL, producing a misleading missing-key failure. That log/report are retained.
The corrected fixture verifies journals, revisions and owner identities and
releases owners immediately after foreground completion, before waiting for
the unrelated tail. Row End measures foreground completion; TrafficEnd records
when background completion was observed. No TTL, renewal or recovery target
changes. All 384 messages and their physical acknowledgement evidence remain
required.

## Results and controls

Final raw-counter four-row race passes in 82.336 seconds. Healthy KV sums are
0.0437–0.0506 seconds without load and 0.0629–0.0727 with it. Delayed sums are
7.2161–8.4171 seconds without and 8.3037–9.3597 with load. Consumer traffic has
not reproduced the mixed invocation's 30.5004-second update sum. This result
cannot rule out other load shapes, failed acquisitions, leader transitions,
heartbeat overlap, larger active groups or combined recovery effects.

The intermediate race (before raw-counter assertions) also passes in 82.497
seconds. The original unchanged eight-row measurement through the shared helper
passes in 43.810 seconds. Vet and workflow YAML checks pass. A compiled omitted
DoubleAck leaves eight warm records retained and fails the physical stream-drain
checker in 9.32 seconds. A compiled fake successful load count, with no actual
traffic, fails raw sequence growth 0 versus required 384 in 6.68 seconds. These
are semantic controls, not build errors or generic timeouts. Patches, rows,
source hashes, logs and final delayed disk trace are retained.

The tier2-mixed workflow adds an independent lease-consumer-pressure job with
its own reports/artifacts. Hosted confirmation is pending; old pressure/recovery
jobs retain their gates. The real 200-seed latency miss and full-matrix/24-hour
release gates remain open. Production runtime code is unchanged from 497ef76;
full simulations and original million timers continue without interruption.
