# Retained mixed seed 12 CI latency failure

[Run 36778973973](https://github.com/AntPAllen/js-wf/actions/runs/36778973973)
passed seeds 1–11 and failed seed 12 at source `9d7e6c0`: terminal p99
50.150568581 seconds. All required workflow results were retained. The files
are the complete downloaded seed-12 artifact, including its fault schedule,
worker operation events, timestamped injected syscalls, pre-fault JSz mappings
and failure server logs. `source.json` hashes these retained inputs and the
parsed summary.

The trace has 4,518 completed delayed syscalls, three unfinished calls and no
unmatched lines. Lease storage accounts for 350 message-file calls and 629
mapped Raft calls. The aggregate injected duration sums are concurrent; they
must not be added or treated as an RPC critical path. The disk victim was
node 1, with an 85 ms injection. The injector configures an 85 ms entry delay. Recorded completed-call durations
can reach approximately 170 ms. The additional delay is not attributed here.

For signal invocation `mixed-06-0`, 52 append renewal observations total
41.09 seconds, including 3.40 seconds at the local lease gate and 37.68 seconds
inside client KV Update calls. One renewal failed. `mixed-02-0` has 53 such
observations totaling 38.59 seconds, including 4.50 seconds of gate waiting;
two failed. The client-side durations do not identify NATS execution time.
One grandchild's terminal append renewal spent 1.448 seconds waiting at the
gate and 1.552 seconds in Update before its three-second attempt timed out.
These observations distinguish local waiting from the KV path; correlation
with injected file writes does not establish request-level causation.

The subsequent heartbeat change removes an unconditional first heartbeat when
an acknowledged renewal is still fresh, using the existing request-start
freshness bound. It retains unconditional per-entry append renewal. A local
seed-12 race replay passed at 17.53 seconds p99, but different hosts and real
interleavings prevent attributing the old failure or claiming a controlled
performance improvement. The earlier failure remains release evidence and
the full consecutive-clean gate remains open.
