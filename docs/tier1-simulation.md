# Tier 1 deterministic simulation: journal, lease, start, signals, and dispatch slices

The first simulator slice runs the production `journal.Store.Append` decision
path through a narrow `journal.AppendPort`. The production port still calls
JetStream. The in-memory port keeps global stream sequences and per-subject
tails, and can inject a request dropped before commit, an acknowledgment lost
after commit, an unchanged-tail CAS rejection, or a competing commit.

`sim.Scheduler` chooses seeded actions and records the enabled set, chosen
action, virtual time, and transport calls in a versioned JSON trace. Replay
rejects an unreachable choice or a changed transport transcript. Waiting
between CAS retries advances virtual time; no wall-clock sleep is needed.
Version 3 traces record a default 100,000-choice limit. Reaching it reports
the last choice, pending actions, and virtual time; a stalled cooperative
actor reports unfinished actors when its caller context expires. Version 2
traces still replay with the default limit. Virtual-clock overflow fails
closed. These diagnostics distinguish a bounded livelock or stalled actor
from a clean seed. `sim.MinimizeFailureTrace` removes forced decisions in
bounded reruns, accepts only the same caller-selected invariant failure, and
verifies that the best generated trace reproduces the exact transport
transcript on disk. A synthetic ten-choice failure shrinks to an earlier
replayable failure. Actor removal and automatic CI shrinking remain.
`sim.RunAppendActors` also yields two real journal append calls at each
transport operation, so 100 seeded two-writer CAS races explore distinct
interleavings without relying on Go goroutine timing. Every run retains one
winner, rejects one stale writer, and replays its exact trace. A pinned race
trace is also byte-identical across separate processes.

The lease slice runs the production `lease.Store` decisions through a narrow
KV port. Its in-memory model has global revisions, CAS create/update/delete,
30-second key expiry, and explicit pre-commit drop or post-commit lost-ack
faults. The default test runs 1,000 seeded sequences of acquire, release,
expiry, orphan reclaim, stale predecessor reads, and renewal. Another 100
seeds each schedule two
acquirers against a fresh key and a stale uninitialized key at individual KV
operations. Every race retains one initialized winner and fences the loser.
The seeded lease trace is byte-identical across processes and replays from
disk. A real three-node fixture checks the model's normal revision, held-key,
renewal, stale cleanup, and stale update behavior. A TCP-proxy fixture holds
the server response after a real lease delete commits, then drops that
acknowledgment; cleanup leaves a successor intact. Three normal repeats and
a race run passed. A separate short-TTL three-node bucket confirms real and
modeled expiry allow a higher fencing epoch and reject the old holder's renew
and cleanup; three normal repeats and a race run passed. A second TCP-proxy
fixture drops an acknowledgment after a real KV create commits and verifies
that the uninitialized orphan is reclaimed with a higher epoch; three normal
repeats and a race run passed. A third TCP-proxy fixture drops a committed
renewal update acknowledgment. Both the real cluster and model report a lost
lease; cleanup removes that owner's uncertain revision, then a successor
acquires a higher epoch and survives a stale cleanup. Three normal repeats
and a race run passed.

Repeated lost-create-ack cluster runs twice observed a different-node KV read
return the new survivor's pre-initialization value (`Epoch=0`) immediately
after `Acquire` acknowledged its epoch update. The contract now waits up to
three seconds for that read to reflect the initialized revision and retries
the orphan claim during the one-second age boundary. The simulator can inject
one predecessor-revision `Get` result; a lease contender that reads it is
fenced by the revision CAS on delete. This models an observed response without
attributing its cause to the server or treating the modeled lag as inevitable.

The client start slice runs the production `Client.Start`, `StartChild`, and
`StartScan.Scan` decisions through narrow transport ports. Its model enforces one invocation per
subject, stores large input objects, and retains run enqueues with message-ID
deduplication. It injects dropped publishes, commits with lost acknowledgments,
stale reads, and lost run-enqueue acknowledgments. The default test runs 1,000
seeded sequences of 20 starts, retries, repair scans, and journaled rescans,
then 100 same-input and 100
different-input two-client races at transport yield points. Another 100 seeds
interleave a lost-ack client start with a repair scan and finish with a final
scan. Races retain one invocation and one run message. A three-node contract fixture compares the
normal start, matching retry, mismatched retry, and retained queue state with
the model; three repeats passed. Another three-node contract fixture stores
an invocation without a run message, confirms dry-run detection, repairs one
run message, and confirms a repeated scan deduplicates; three repeats and a
race run passed. The model also checks purged invocation sequence holes and a
lost repair-enqueue acknowledgment. Existing three-node fixtures also cover
network-lost start acknowledgments, absent-publish retry, and large input.
The start-and-repair scenarios do not yet run the leased scan loop or its
persisted cursor. The model now expires `WF_RUN` message IDs after the
configured duplicate window using virtual time (two minutes by default).
A model scan proves that a still-unstarted invocation gets another run
message after expiry and stops being reenqueued once a journal exists. A
three-node contract confirms that provisioned `WF_RUN` uses the two-minute
window and that a separate short-window stream accepts the same message ID
again after expiry while deduplicating it inside the window.

The signal repair slice runs production `SignalScan.Scan` through a narrow
port for retained signal and invocation reads, journal reads, and run enqueue.
Its in-memory transport shares the start model's invocation stream, run
message deduplication, and enqueue faults. The default 1,000-seed workload
covers 20 signals per seed: an interrupted wakeup, lost or dropped enqueue
acknowledgments, already consumed or terminal journals, stale invocation
generations, purged signal sequence holes, absent invocations, and a scan
repeated after the deduplication window. Eligible signals produce a retained
wakeup; consumed, terminal, stale, purged, and absent targets do not. The
trace replays exactly and seed 42 is byte-identical across processes. A
three-node contract compares dry-run detection, repair, repeated-scan
deduplication, stale-generation rejection, and a deleted signal sequence
hole; three repeats passed. A second 1,000-seed workload interleaves a
fixture signal publish with each production scanner transport call. It covers
publication before the signal read, between a missing read and stream info,
and after the scan; repeated scans from the returned cursor repair the
wakeup in all three orders. The cooperative trace also replays exactly and
is byte-identical across processes. A third 1,000-seed workload now runs
production `Client.Signal`, `SignalWithOptions`, `SignalToGeneration`, and
`SignalWithStart` through a narrow port backed by the same signal and run
model. It covers request loss before commit, committed signals with lost
acknowledgments, lost or dropped wakeup enqueue, matching and mismatched
idempotency retries, terminal admission, stale generations, consumed-signal
verification after purge, and client-to-scanner repair of an unknown outcome.
Each case retains the expected signal and run messages, and seed 42 replays
from disk and is byte-identical across processes. Focused tests also cover
large signal Object Store spill. A three-node contract compares
`SignalWithStart`, matching and changed retries, `RequireRunning`, and
retained signal and run counts; three repeats passed. Existing real TCP-proxy
and SDK-boundary tests cover lost signal acknowledgments and safe retry.
The earlier publication/scan race uses a fixture publisher. A fourth
1,000-seed workload now interleaves production client signal calls with
production scanner calls at each transport operation. It injects a lost
signal publish acknowledgment or a lost/dropped wakeup enqueue, then scans
from the returned cursor until exactly one retained wakeup exists. The
trace replays exactly and is byte-identical across processes. The leased
signal loop and worker signal drain remain outside the model.

The dispatch slice runs the production `worker.RunPartition` fetch/retry loop
through a narrow consumer port with a supplied handler callback. Its model
retains run messages, tracks delivery count, explicit ack/nak/progress, virtual
AckWait, and consumer recreation after injected leader-change errors. The
default test runs 1,000 seeds of 20 messages with immediate ack, one
unacknowledged delivery, or a progress call before redelivery. Each scenario
acks all 20 exactly once, drains the modeled queue, and replays its trace;
seed 42 is byte-identical across processes. A focused model test proves that
an acknowledgment can commit while its response is lost. A three-node
contract compares one delivery, AckWait redelivery, and final ack state with
the model; three repeats and a race run passed. Existing three-node worker
tests separately cover consumer-leader kills and a live handler's progress
heartbeats. A second three-node contract closes the first client with a
delivery unacked, reopens the same durable from another node, and compares
redelivery and acknowledgment with the model. Two clients then fetch distinct
new messages from that durable; three repeats and a race run passed. A direct
three-node test then found a model discrepancy: JetStream accepts an ack from
an older delivery after redelivery and clears the message, then accepts the
newer's duplicate ack. The model now matches; three normal repeats and a race
run passed. This slice exercises the dispatch loop and consumer contract; the
worker handler's lease, journal, signal, and timer decisions remain outside
the model. A second 1,000-seed workload now runs two production partition
loops against one modeled durable. It yields at creation, fetch, wait,
acknowledgment, nak, and progress calls; interleaves an injected leader-change
error, unacknowledged first deliveries, and final acknowledgments; and drains
all ten messages in every seed. Traces replay exactly and are byte-identical
across processes. It exposed a clean-shutdown gap: cancellation during
consumer creation returned `context canceled`; `RunPartition` now exits
normally when its context is already canceled. The real shared-durable
contract above confirms separate clients receive distinct messages and
redelivery survives client replacement. The full handler and server-driven
leader timing remain outside the modeled comparison.

Run the fixed fault cases and 1,000 seeded scenarios:

```sh
go test ./sim -count=1
go test -race ./sim -count=1
```

Set `FAULT_SEED` to select a seed for the separate-process trace test. A
failing 1,000-seed scenario test prints its seed and saves a trace under a
temporary directory, or to `FAULT_TRACE_OUT` when set. To replay a saved trace:

```sh
FAULT_TRACE=/path/to/trace.json go test ./sim -run '^TestReplayFaultTrace$' -count=1
```

The default suite runs 1,000 journal scenarios, each with 100 starts and a
mix of transport outcomes, plus 100 two-writer races. On this VM the initial
journal-only simulator package completed in 0.4 seconds without the race
detector and 6.1 seconds with it. The trace
test produced byte-identical JSON in two separate processes and rejected a
changed enabled set. The unit CI job now runs this package and uploads a
failure trace when one is written.

`TestSimJournalAppendContractAgainstRealCluster` checks successful writes,
global sequence gaps between subjects, retained bytes, and stale CAS results
against a real three-node stream. Existing real-cluster tests cover network
lost acknowledgments and injected unchanged-tail rejections. This comparison
is limited: the model does not yet run the full worker handler, signals, timers, retention,
Raft elections, or disk storage. The journal's five-second attempt
deadline still uses wall time; only CAS retry waits are virtual in this first
slice. A discrepancy seen only on real
NATS is a candidate model gap or environment/server issue, not proof of a
server defect. See the [Tier 1 plan](implementation-plan.md#distributed-verification)
for the remaining transport contract and invariant gates.
