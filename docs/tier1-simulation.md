# Tier 1 deterministic simulation: journal and lease slices

The first simulator slice runs the production `journal.Store.Append` decision
path through a narrow `journal.AppendPort`. The production port still calls
JetStream. The in-memory port keeps global stream sequences and per-subject
tails, and can inject a request dropped before commit, an acknowledgment lost
after commit, an unchanged-tail CAS rejection, or a competing commit.

`sim.Scheduler` chooses seeded actions and records the enabled set, chosen
action, virtual time, and transport calls in a versioned JSON trace. Replay
rejects an unreachable choice or a changed transport transcript. Waiting
between CAS retries advances virtual time; no wall-clock sleep is needed.
`sim.RunAppendActors` also yields two real journal append calls at each
transport operation, so 100 seeded two-writer CAS races explore distinct
interleavings without relying on Go goroutine timing. Every run retains one
winner, rejects one stale writer, and replays its exact trace. A pinned race
trace is also byte-identical across separate processes.

The lease slice runs the production `lease.Store` decisions through a narrow
KV port. Its in-memory model has global revisions, CAS create/update/delete,
30-second key expiry, and explicit pre-commit drop or post-commit lost-ack
faults. The default test runs 1,000 seeded sequences of acquire, release,
expiry, orphan reclaim, and renewal. Another 100 seeds each schedule two
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
is limited: the model does not yet run workers, consumers, timers, retention,
Raft elections, or disk storage. The journal's five-second attempt
deadline still uses wall time; only CAS retry waits are virtual in this first
slice. A discrepancy seen only on real
NATS is a candidate model gap or environment/server issue, not proof of a
server defect. See the [Tier 1 plan](implementation-plan.md#distributed-verification)
for the remaining transport contract and invariant gates.
