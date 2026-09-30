# Tier 1 deterministic simulation: journal, lease, start, signals, timers, dispatch, and worker slices

The first simulator slice runs the production `journal.Store.Append` decision
path through a narrow `journal.AppendPort`. The production port still calls
JetStream. The in-memory port keeps global stream sequences and per-subject
tails, and can inject a request dropped before commit, an acknowledgment lost
after commit, an unchanged-tail CAS rejection, or a competing commit.
The model now also supplies a narrow live-message `ReadPort` to production
`journal.Store.Read`. The same index, epoch, and terminal validation runs over
retained in-memory messages. Snapshot objects remain a real-JetStream read
path. A focused control accepts a global sequence hole between two entries
of one journal and rejects a missing logical index after a bounded two
seconds of virtual retry time.

The existing three-node journal contract now compares the modeled and real
`Read` records and tail after interleaving two subjects, including their
global stream-sequence hole. Three repeats and a race run passed.

Long reads now use the same production `BatchReadPort` decision path in the
model and on JetStream after 64 live entries. The in-memory cursor returns
subject-filtered stream sequences and can end a pull with a no-responder or
consumer-deleted error after a partial batch. A 1,000-seed workload reads 80
entries interleaved with another subject and covers clean reads, partial
delivery, cursor replacement after repeated faults, fail-closed retry
exhaustion, and a deleted middle entry that returns `ErrGap` after 80 virtual
25 ms whole-read retries. Batch-fault 50 ms retries also advance virtual time;
the first ten seeds replay through the shared trace dispatcher, and seed 42
produces byte-identical traces across processes. The
three-node 300-entry contract now mirrors the same publish order in the model
and compares every record and tail after both production read paths.
A local 100,000-seed run of this batch-read workload passed in 167.62 seconds;
this is one workload, not the full Tier 1 release gate.

A separate 1,000-seed workload models timed-out or empty pulls while a direct
tail probe still finds the next entry. Production reads replace the cursor
after two empty pulls, resume from the last verified sequence, and return a
bounded timeout after six consecutive empty pulls rather than spinning until
the caller's deadline. The workload covers stalls before and after partial
delivery, permanent stalls, exact replay, and cross-process trace identity;
seed 42 is pinned. It fails against the former read loop. A real 500-child
leader-restart reproduction timed out at 303 seconds while its parent was
already suspended and acknowledged; three local runs after cursor replacement
completed all children and retained audits in 10.4, 17.1, and 10.8 seconds.
This isolates the observer's lack of progress; the precise real transport
condition that stopped its cursor remains unconfirmed.

A pinned worker-kill lease trace now reproduces the five-container recovery
floor using the production `lease.Acquire` and `journal.Append` decisions over
virtual transports. The replacement receives `ErrHeld` at 11,999 ms after
the killed owner's last lease write, acquires at the provisioned 12,000 ms
TTL, and appends one step outcome and terminal entry under a higher epoch.
At virtual 45,000 ms, the paused old owner resumes: production `lease.Renew`
returns `ErrLost`, its stale production journal append returns `ErrStale`,
and the four retained entries remain unchanged. The trace is pinned in
`sim/testdata/regressions/worker-pause-lease.json`; exact disk replay and a
focused race run pass. Earlier 30-second and 20-second traces corresponded to
31.1-second and 21-second observed recoveries. The current 12-second trace
proves the modeled lease boundary; real recovery at this TTL is measured
separately. Process signals
and server behavior remain outside the model.

A second pinned lease trace isolates the mixed seed 55 retained-lease delay.
The old owner writes a partial journal and renews its lease at virtual 9,000 ms.
A delete dropped before commit makes production `lease.Release` fail, and a
transport-lost cleanup read leaves the lease present. The replacement receives
`ErrHeld` one millisecond before the renewed 12-second TTL expires, acquires
at 21,000 ms with a higher epoch, completes the journal, and rejects the old
owner's cleanup, renewal, and stale append. The trace is
`sim/testdata/regressions/worker-canceled-retained-lease.json`. It proves the
runtime's lease and fencing behavior under those modeled replies; the real
seed 55 trace did not establish which release or cleanup call failed.

The production `journal.Store.Read` can now load a compacted prefix through a
narrow modeled manifest and Object Store read port, then join it to retained
live entries. A 1,000-seed workload advances the snapshot manifest twice,
purges only entries below each fixed sequence bound, and checks the original
logical records and tail after both reads. A stale manifest read and a
transient object read retry on virtual time. A corrupt snapshot object fails
closed with `ErrGap` after bounded virtual retries. A three-node contract
compares real and modeled snapshot metadata, object bytes, reconstructed
records, and tails after two compactions. Snapshot creation and purge still
run through real JetStream in production. A second 1,000-seed workload now
runs production `MaybeSnapshot`, `SnapshotPrefix`, and `PurgeSnapshot` through
modeled object upload, manifest revision CAS, fixed-bound journal purge, and
consumed-signal purge. Dropped requests and committed writes with lost
acknowledgments at each boundary retry to the same logical journal, one
manifest, and no retained consumed signal. The three-node contract also runs
production snapshot writes against both transports and compares both
compactions byte for byte.

`sim.RunSnapshotActors` yields each production snapshot read, object write,
manifest CAS, and bounded purge to the cooperative scheduler. A 1,000-seed
two-compactor workload starts from one compacted journal and races two
production `SnapshotPrefix` calls with different retained suffix lengths.
The losing writer receives `ErrSnapshotStale` when its CAS conflicts; the
manifest advances one or two revisions depending on whether the second
compactor reads the new revision. A fresh read reconstructs every original
record, and the live suffix matches the winning manifest. Fault choices
drop or hide acknowledgments for the object upload, manifest update, and
bounded journal purge. A later compactor call repairs uncertain writes.
Pinned seeds 1 and 4 cover CAS conflict with a dropped purge and a lost
manifest acknowledgement, including the longer-prefix writer losing;
seed 42 matches across processes.

A three-node contract now mirrors the two-compactor race with production
JetStream ports. A barrier holds both writers after they read the same
manifest revision. Ten distinct journals per run check exactly one successful
CAS and one `ErrSnapshotStale`, the winning manifest, every reconstructed
logical record, and the physical suffix left by its fixed-bound purge.
The two writers use different retained suffix lengths, so a stale purge with
the longer cutoff would cause a visible gap.

The production worker now calls modeled `MaybeSnapshot` after a successful
delivery when its journal has a snapshot write port. A 1,000-seed workload
runs 130 `wf.Run` steps in one handler, triggering the ordinary 256-entry
snapshot cadence. Faults drop or hide acknowledgments for the object upload,
manifest create, and bounded journal purge; terminal KV uncertainty and a
consumer-leader change are also covered. Redelivery completes compaction
without rerunning any of the 130 effects. The final logical journal has 262
entries, the live stream keeps 16, and the reconstructed I1/I2/I3/I6
snapshot passes. Seed 42 is pinned and replays across processes.

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
replayable failure. The minimizer now also tries removing cooperative
actors discovered in the trace. A minimized trace records disabled actor
names and replays the same failure and transport transcript from disk; a
two-actor probe removes its irrelevant actor. Ordinary traces omit the new
field and keep their existing serialized form. On a unit CI failure, a
bounded follow-up test now loads `sim-failure.json`, requires the same error
and exact original transcript, then writes `sim-failure-minimized.json`. Both
files are uploaded; a failed or unreproducible shrink leaves the original
trace available.

The seeded workload loops default to seeds 1–1,000. `SIM_SEEDS` accepts a
larger upper bound for every workload, up to 1,000,000; smaller values fail
closed because some coverage checks need the full first 1,000 seeds. A local
`SIM_SEEDS=10000 go test ./sim -count=1 -timeout=20m` run passed in 334.5
seconds of Go test time (334.9 seconds wall) on the expanded four-CPU VM.
`tier1-extended` is a manual CI workflow with 10,000 and 100,000 seeds per
workload and the same failure-trace artifact path. The per-push gate remains
at 1,000 until the larger run is measured on CI hardware against the plan's
few-minute condition. With `SIM_COVERAGE_SUMMARY=1`, the test runner reports
model version, generated schedules, scheduler choices per second, transport
events, and final virtual-time buckets. A local default run produced 49,602
generated schedules and 1,503,118 scheduler choices in 34.1 seconds;
22,808 schedules ended at virtual time zero and 2,894 at one minute or more.
The first 10,000-per-workload GitHub Actions run passed at commit `77fac0d`:
378.9 seconds of Go test time and 381.8 seconds wall time. This exceeds the
plan's few-minute condition for making 10,000 seeds per workload a push gate.
The default 1,000-per-workload suite already generates roughly 49,600
schedules per push in aggregate; that count is distinct from 10,000 seeds
for each workload.
The expanded [100,000-per-workload CI run](https://github.com/AntPAllen/js-wf/actions/runs/36619464017)
passed at `e82897b`: 5,000,602 generated schedules, 150,227,003
scheduler choices, and 1,177,902,920 modeled transport events in 59m20.3s
of Go test time. This reaches the release seed count for the implemented
model slices; it does not close unmodeled transport edges or real-cluster
proofs.
CI now replays a pinned corpus covering committed and dropped CAS unknowns,
two-worker dispatch, competing suspended scanners, signal, suspended, and child
notification liveness, outcome persistence, integrated short-handler execution,
retained-state checks, and workflow determinism. Go runs package tests from `sim/`, so the relative
`sim-failure.json` output lands under the uploaded artifact path.
An operation context can be cancelled while its actor is waiting for a
scheduler turn. The scheduler now resolves every submitted turn and lets the
transport observe that cancellation inside the chosen action; this removed
an OS-select race that made two-worker dispatch traces diverge on replay.
A focused cancellation probe and 20 repeated race-instrumented two-worker
runs now replay exactly.
`sim.RunAppendActors` also yields two real journal append calls at each
transport operation, so 100 seeded two-writer CAS races explore distinct
interleavings without relying on Go goroutine timing. Every run retains one
winner, rejects one stale writer, and replays its exact trace. A pinned race
trace is also byte-identical across separate processes.
A further 1,000 seeded two-writer races drop one publish before commit or
hide its acknowledgment after commit. Each schedule retains exactly one
next journal entry. A dropped publish leaves one caller with `ErrUnknown`
and the other with success; a hidden acknowledgment leaves the physical
winner with `ErrUnknown` and the other caller with `ErrStale`. Exact trace
replay and cross-process equality pass. This is the
modeled distinction needed when a real CAS race returns an ambiguous reply.

A further 1,000-seed two-writer schedule injects one transient subject-tail
lookup failure before either CAS publish. The caller sees `ErrUnknown`,
retries the same entry, and the schedule retains one winner while the other
writer is stale. Seed 42 is pinned and byte-identical across processes. A
three-node fixture injects the tail lookup error before the publish barrier
and checks the same outcomes; the 10,000-round race now caches stream handles
across rounds, refreshing them after leader restarts, and retries a writer
only when it has not yet reached the publish barrier.

The lease slice runs the production `lease.Store` decisions through a narrow
KV port. Its in-memory model has global revisions, CAS create/update/delete,
30-second key expiry, and explicit pre-commit drop or post-commit lost-ack
faults. The default test runs 1,000 seeded sequences of acquire, release,
expiry, orphan reclaim, stale predecessor reads, and renewal. Another 100
seeds each schedule two
acquirers against a fresh key and a stale uninitialized key at individual KV
operations. Every race retains one initialized winner and fences the loser.
Lease actors can now read distinct wall clocks while sharing the same
server-clock KV expiry and revisions. A 1,000-seed schedule gives contenders
offsets of -400 ms and +400 ms around the one-second orphan-reclaim boundary.
At 500 ms of server time both must leave the uninitialized key held; at
800 ms only the fast clock can initiate reclaim, and revision CAS still
permits one initialized winner. Both boundary traces are pinned on disk,
and seed 1 is byte-identical across processes. Other modeled actors still
need explicit wall-clock offsets. A three-node contract gives the same real
and modeled lease decisions -2 s and +2 s local offsets over server-clock
KV revisions: the slow contender leaves the orphan held, the fast contender
reclaims it, and both stores retain an initialized higher epoch. Three
normal repeats and a race run passed.

A separate three-node expiry race starts 32 production lease acquirers through
clients pinned to all three nodes after a short-TTL key expires. Before adapter
normalization, one contender won, 21 returned `ErrHeld`, and ten leaked raw
JetStream CAS code 10164 from the expired-key `Create` path. The production KV
adapter now maps that Create response to `ErrKeyExists`, which the unchanged
production lease decision classifies as `ErrHeld`. Three real-cluster repeats
and a race run passed with one higher-epoch winner and 31 held losers. The
in-memory KV model already exposes the normalized conflict contract; a unit
test also preserves the underlying API error while distinguishing unrelated
transport errors.
The matching cooperative Tier 1 workload now schedules 32 production acquirers
at every KV operation after an initialized holder expires. Each of 1,000
default seeds must retain exactly one higher-epoch winner and return `ErrHeld`
to all 31 losers. The expired holder's renewal and cleanup must be fenced
without changing the successor. The first ten seeds replay exactly, seed 42
is byte-identical across processes and pinned in the regression corpus, and
the workload participates in `SIM_SEEDS` extended runs. This checks the
normalized contract and production acquisition decisions; raw server API
error classification remains covered by the adapter unit test and real fixture.
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

The retained-state integrity checker now exposes `CheckSnapshot` for modeled
invocation subjects, reconstructed journal records, and terminal KV values.
It shares journal validation with the real JetStream checker. Deliberate
mutations for duplicate starts (I1), two workers sharing an epoch and a
descending epoch (I2), a completion without a request (I3), and a changed
terminal value (I6) each make the checker fail. The real checker also rejects
two recorded worker IDs in one nonzero epoch. Suspended timer and signal
wakeup checks and I5 negative controls are described below; integrated
worker advancement remains open.

A separate 1,000-seed workload now runs production `Client.Start` and
`journal.Store.Append` against retained in-memory invocation and journal
transports, with dropped and hidden-ack step completions. It reloads each
retained journal through production `journal.Store.Read` and compares it to
the independent raw-state snapshot. It runs the
production worker's terminal KV persistence decision through the modeled
revision-CAS port, including a lost create acknowledgment and idempotent
retry. It reconstructs the checker input from the transports'
retained contents, and runs `CheckSnapshot` after each of five terminal
schedules per seed. The checker report enters the trace; exact disk replay
and byte-identical cross-process traces pass. The full worker handler is
exercised in the integrated workloads below.

Another 1,000-seed workload runs production `Client.Start`, `wf.Run`, and
`journal.Store.Append` and `Read` against retained in-memory transports. For five
three-step completions per seed, it checks the reconstructed state, replays
the real recorded step requests without rerunning an effect, then changes a
step name and input separately. Both deliberate I4 mutations return
`ErrNonDeterministic`. Its checker result and replay verdict enter the trace;
exact disk replay and byte-identical cross-process traces pass. Its terminal
write also uses the production worker decision. Dispatch and lease fencing
remain outside this workload.

A further 1,000-seed terminal-outcome workload injects dropped or hidden KV
create and update acknowledgments, retries persisted outcomes, replaces old
purge tombstones by revision CAS, fences a same-generation tombstone, and
rejects a changed terminal payload. Its retained KV value and decision enter
the trace; seed 42 is pinned and replays across processes. The production
worker handler is exercised separately below through an integrated
short-handler schedule.

A three-node contract compares modeled and real normal outcome creation,
idempotent retry, changed-result rejection, same-generation tombstone fencing,
and newer-generation replacement. Three repeats passed. A TCP proxy hides a
committed real KV create acknowledgment; retry leaves its original revision
and payload intact, matching the model's lost-ack cut. Three normal repeats
and a race run passed.

A second proxy contract hides a committed tombstone-replacement update
acknowledgment. The model can independently lose the following confirmation
read, matching the error path while the real client connection is cut. Both
paths retry without another KV revision or a changed result. Three normal
repeats and a race run passed.

A 1,000-seed tombstone sweep workload now runs the production expiry,
generation, and KV revision checks through a narrow invocation lookup and
delete port. The full production sweep also enumerates modeled state keys,
skips non-tombstone entries, and makes an idempotent second pass. It covers a
missing or held old invocation, a reused ID, an unexpired marker, a dry run,
a state replacement before delete, and dropped or committed deletes with lost
acknowledgments. The dropped delete is retried
after faults heal; the lost acknowledgment leaves the key absent. Two traces
pin the lost-ack and replacement cuts, and exact replay, cross-process
traces, and a race run pass. A three-node contract compares the complete real
and modeled sweep results and retained keys for absent, held, reused, and
unexpired generations; three repeats and a race run passed. Production
retention purge is covered in a separate workload below.

A separate 1,000-seed workload now runs production
`SweepBlobsQuiescentWithPort` across modeled `WF_INV`, `WF_SIG`, and `WF_JRN`
messages, KV values, snapshot objects, and Object Store listings. It retains
shared input, signal, step, terminal, and snapshot references; skips stream
sequence holes and young or unmanaged objects; and reclaims orphans after
their references are removed. Dropped and lost delete acknowledgments,
temporarily unavailable snapshot reads, and corrupt snapshot bytes abort
the pass at the expected cut, then recover after the fault clears. Two pinned
traces cover a lost delete reply and a missing snapshot read; exact replay,
cross-process traces, and a race run pass. A three-node contract compares
the full real and modeled sweep result before and after removing retained
references; three repeated runs and a race run passed. Reference removal in
this workload is a fixture action; a separate workload below runs production
`retention.Purge` stages.

A 1,000-seed combined workload now runs production `PurgeWithPort` and
`SweepBlobsQuiescentWithPort` over shared retained streams, `WF_STATE`,
lease KV, and Object Store objects. It faults the purge marker, signal and
journal purges, generation-scoped fallback timer purge, snapshot manifest
delete, tombstone CAS, purge-event publish, and final invocation purge.
Production `journal.Store.SnapshotPrefix` first writes the snapshot object and
manifest and purges the covered journal prefix through the same modeled
transport. The purge then reads its terminal journal through production
`journal.Store.Read`: that snapshot supplies the prefix and one live entry
supplies the terminal suffix. A one-read object visibility delay recovers inside the
journal reader; persistent corrupt object bytes fail before the purge marker
is written, then a repaired object permits retry. After retry, it checks one
generation-bound purge event, no old invocation,
signals, journal, or current-generation timer, a retained other-generation
timer, a cleared marker and snapshot manifest, and reclamation of five
unreferenced runtime objects. A second purge is idempotent. The workload
also catches and now fixes a transient state read after the invocation is
gone being mislabeled `ErrNotFound`; the transport error reaches the caller
and a retry succeeds. Five traces pin lost purge-event and invocation
replies, the transient state read, the object visibility delay, and corrupt
snapshot bytes. Exact replay, cross-process traces, and a race run pass. A
three-node contract compares production purge and blob sweeping, including
the corrupt snapshot failure boundary and recovery, with the model; three
repeated runs and a race run passed. A further 1,000-seed cooperative
schedule interleaves production snapshot reads and writes with a production
retention purge attempt while a modeled worker holds the invocation lease.
Purge returns `ErrActive` at every held-lease cut; after the snapshot
finishes and the worker releases its lease, purge and quiescent blob sweep
complete. Two traces pin the fenced and after-release orderings. A three-node
fixture blocks a real snapshot object upload while a real purge attempts the
same invocation, then checks journal reconstruction, purge, and blob
reclamation after release. Another 1,000-seed cooperative schedule runs
production journal `Append` concurrently with production `SnapshotPrefix` on
the same retained transport, with a purge attempt fenced by the worker lease.
The snapshot can cover either of two prefixes; its fixed purge bound always
retains the later terminal append, and production `Read` reconstructs all
entries before retention purge and quiescent blob sweep. The writer also
re-reads after a dropped append request or a committed append with a hidden
acknowledgment, retrying only when the terminal entry is absent. Three traces
pin both cutoffs and both uncertain-reply branches. A three-node fixture
blocks snapshot upload, exercises clean, dropped, and hidden-ack terminal
appends, then checks the live suffix, reconstruction, purge, and reclamation.
A further 1,000-seed schedule runs the production worker handler and an
independent production compactor against one modeled journal. The handler
executes 32 `wf.Run` effects while the compactor starts after a live prefix
appears. Transport calls interleave under the cooperative scheduler, including
dropped and hidden-ack object, manifest, and purge writes. The handler must
complete once, a later compaction repairs uncertain writes, and production
`Read` reconstructs all 66 entries from the snapshot and four live entries.
The first ten seeds replay exactly, seed 1's dropped purge is pinned on disk,
and its trace is byte-identical across processes. Online blob sweeping remains
open; that workload still requires quiescent writers.

The integrated short-handler workload runs production `Client.Start` and
`Worker.handle` over a shared modeled run stream and durable consumer. Its
invocation, lease KV, journal append and read,
signal drain, and terminal outcome KV operations use the existing narrow
ports. Five workflows per seed execute a `wf.Run` effect and finish through
the worker's lease release and message acknowledgment path. Across 1,000
seeds, faults drop or hide an initial run enqueue and repair it with production
`StartScan`, drop a step-completion request, hide a committed completion or
terminal KV reply, hide a run acknowledgment, or change the consumer leader.
A dropped completion reruns its effect once; committed-but-unacknowledged
completion and result writes replay without a second effect. The final raw
snapshot passes I1/I2/I3/I6 checks. Seed 42 is pinned and replays across
processes and under the race detector. Committed `WF_RUN` enqueues feed the
modeled consumer directly, including publishes with lost replies. Timer scheduling, snapshots,
large result blobs, running cancellation, and cooperative heartbeat turns
are exercised in separate slices or remain open as described below.

A separate 1,000-seed running-cancellation workload starts a production
`Worker.handle` effect that waits for its context. A committed cancel signal
is delivered to the worker’s production notification handler after a
stale-generation notification is ignored. Dropped publishes are retried;
committed signals with lost replies and uncertain wakeup enqueues still
interrupt the effect. The worker journals one `SignalConsumed` and `Failed`,
without a `StepCompleted`, and the retained I1/I2/I3/I6 check passes. A
lost-ack seed is pinned, and exact replay, cross-process traces, and a race
run pass. The same production cancel-generation lookup now reads retained
`WF_SIG` through a narrow port. Seeded modes withhold the core notification,
advance virtual time to a 15-second poll, and ignore a stale retained
generation before recognizing a matching one. A second pinned trace covers
the stale-then-matching durable poll. A three-node fixture removes the core
subscription before delivery and verifies that the real periodic poll
interrupts the effect and leaves one canceled terminal journal.

A 1,000-seed heartbeat workload holds a production `wf.Run` effect while
virtual ticks drive the worker’s real lease-renewal and consumer-progress
decisions. At the original three-second AckWait boundary, an unprotected
message redelivers, while early, late, and repeated progress reports extend
its deadline. The worker then completes one effect and one terminal journal;
the retained I1/I2/I3/I6 check passes. Seed 42 is pinned and replays across
processes and under the race detector. A three-node contract compares the
model with real JetStream across the original and extended deadlines.
A second 1,000-seed workload hands an unfinished step between two production
workers. A dropped or unacknowledged lease renewal, failed progress write,
or closed tick source cancels the first blocked effect. It cleans up its lease,
publishes a durable handoff run keyed by the original stream sequence, and
naks the original delivery. The successor receives the handoff and later
redelivery of the original message,
replays `StepRequested` under a higher epoch, executes the effect once more,
and writes the only terminal result. I1/I2/I3/I6, exact replay, cross-process
traces, and the race detector pass. The handoff is deduplicated across publish
retries. A dropped nak leaves the original delivery pending until AckWait,
while the handoff run lets the successor start sooner; the model checks both
deliveries drain to the same terminal result. Pinned traces cover a hidden
renewal reply, a failed progress write, and a dropped nak. A three-node
dispatch contract now checks the modeled dropped-nak server state against a
real consumer whose nak is withheld before send: the handoff arrives first,
the original redelivers after AckWait, and both messages drain. Three normal
runs and a race run passed. A second three-node contract checks that a
successful one-second `NakWithDelay` redelivers before the four-second
AckWait and drains after ack; three normal runs and a race run passed. A
three-node
worker-level fixture hides
a committed lease-renewal reply during a blocked effect and checks the same
handoff, terminal result, and drained run queue; three repeated runs and a
race run passed. Another three-node worker-level fixture injects a failed
`InProgress` response at the real dispatch boundary while keeping the lease,
journal, nak, redelivery, and successor execution on JetStream. It verifies
the first effect stops, only `StepRequested` remains, the successor runs under
a higher epoch, and the run message drains. Three repeated runs and a race
run passed. The same fixture closes an injected heartbeat tick source while
the effect is blocked and verifies its cancellation, nak, successor replay,
and queue drain. Both modes passed three repeated runs and a race run.
Cooperative actor scheduling of concurrent heartbeat and failure turns
remains open.

The integrated signal workload follows one workflow across a suspension and
resume. Production `Client.Start` enqueues its first run, `Worker.handle`
journals `Suspended`, and production `Client.Signal` publishes `go` and enqueues
the resumed run. The same worker drains and journals the signal, replays its
earlier wait, and writes `StepCompleted`, `Completed`, and the terminal result.
Across 1,000 seeds it covers a lost signal publish acknowledgment, dropped or
unacknowledged wakeup enqueue repaired by production `SignalScan`, dropped or
unacknowledged completion append, lost run acknowledgment, and a consumer
leader change. The final retained snapshot passes I1/I2/I3/I6, and signal
liveness finds no eligible undrained signal. Seed 42 is pinned and replays
across processes. Timer, snapshot, large blob, cancellation, and cooperative
heartbeat turns remain outside this workload.

The integrated native timer workload runs production `Client.Start`,
`Worker.handle`, `wf.Timer`, and `TimerHandle.Await` over modeled clock, timer
publish, and durable consumer ports. A first delivery journals `Suspended`;
virtual time advances to the recorded fire time and the native timer target
delivers with its generation, step headers, and server timestamp. The same
worker replays the wait and writes its terminal journal and result. Across
1,000 seeds the schedule publish is clean, dropped, or committed with a lost
reply; other seeds lose a completion append or timer-run acknowledgment or
change the consumer leader. No timer fires before its due time, each scenario
retains one native timer source and one terminal result, and the final snapshot
passes I1/I2/I3/I6. Seed 42 is pinned and replays across processes. Fallback
timer routing and a simultaneous timer/signal wait remain separate slices.

The integrated fallback timer workload runs the same production worker in
fallback mode. Its timer request is retained in `WF_TIMER`; production
`FallbackTimerScan.Scan` sees the due record, publishes the generation-bound
`WF_RUN` wakeup, and deletes the timer. The committed wakeup feeds the same
modeled durable consumer, including when its acknowledgment is lost. Across
1,000 seeds, a dropped or unacknowledged timer schedule is retried by worker
redelivery; dropped or unacknowledged scanner wakeup and timer-delete calls
are repaired by a second scan. Completion append, run acknowledgment, and
consumer leader faults also finish with one terminal result. The final raw
snapshot passes I1/I2/I3/I6, and the trace replays across processes. The
leased fallback poller now runs in a second 1,000-seed integrated workload:
the first production `RunLoopWithPort` instance acquires its lease, scans the
due timer, and attempts a cursor save; a replacement instance loads that
cursor and repairs dropped or hidden-ack wakeup, delete, and cursor writes.
The retained wakeup then resumes the production worker to one terminal
result. Seed 42 pins a dropped delete and replays byte-identically across
processes. Simultaneous timer/signal choices are covered separately.

The integrated timer/signal select workload runs production `wf.Timer` and
`TimerHandle.SelectSignal` through the modeled worker. Some seeds consume a
signal before the timer is due; others leave the signal buffered until both
are ready, where signal priority must win. Timer-only seeds fire the timer
branch. It covers uncertain signal publishes and enqueues, a lost timer
schedule acknowledgment, and consumer-leader movement. The worker records
the selected branch in its journal and terminal result. After a signal win,
the later timer target is acknowledged as a cancelled no-op without changing
the terminal state. Across 1,000 seeds, retained I1/I2/I3/I6 state, exact
result bytes, signal liveness, branch metrics, and cross-process trace replay
pass.

The integrated child workload starts a parent through production `Client.Start`.
The parent handler calls `wf.CallAsync` and journals a suspended
`wf.AwaitPromise`. A production child handler runs one effect and persists its
result; `Worker.handle` then sends the generation-bound terminal notification
through the same modeled signal and run streams. The parent resumes, journals
the consumed signal, and returns the child's result. Across 1,000 seeds,
faults drop or hide the child step-completion acknowledgment, terminal KV
acknowledgment, notification signal, or parent wakeup; a consumer-leader
change also retries safely. The parent and child have one retained invocation
and terminal result each, the parent receives one notification, the final
snapshot passes I1/I2/I3/I6, and seed 42 replays across processes.

The integrated large-result workload runs production `wf.Run` and
`Worker.handle` through a narrow result Object Store port. Each handler
produces a result above both step and terminal inline limits, so both writes
use content-addressed object names. Across 1,000 seeds, a step or terminal
object write is dropped or committed with a lost acknowledgment, a recorded
step object read is temporarily unavailable, a step completion acknowledgment
is lost, a terminal KV acknowledgment is lost, or
the consumer leader changes. An uncertain step object write is retried by
worker redelivery with the same object name; the worker does not journal a
permanent failure for an ambiguous write. The workload checks two retained
objects, their hashes and bytes, one terminal KV result, effect counts, and
the I1/I2/I3/I6 snapshot. Seeds 1 and 42 are pinned; seed 1 covers
redelivery after a temporarily unavailable recorded object. Traces replay
across processes.

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

An additional modeled fault returns JetStream 10158 while a wakeup message ID
is in process. Production `Client.Enqueue` and `Client.Signal` retry the same
ID after 25 ms of virtual time and retain one run message. The retry has a
two-second wall-clock and 80-conflict bound in production. The 20-signal
seeded pipeline now chooses this fault alongside lost acknowledgments and
dropped enqueues, then checks that a matching retry retains one wakeup.

The start-and-repair scenarios do not yet run the leased scan loop or its
persisted cursor. The model now expires `WF_RUN` message IDs after the
configured duplicate window using virtual time (two minutes by default).
A model scan proves that a still-unstarted invocation gets another run
message after expiry and stops being reenqueued once a journal exists. A
three-node contract confirms that provisioned `WF_RUN` uses the two-minute
window and that a separate short-window stream accepts the same message ID
again after expiry while deduplicating it inside the window.

`CheckStartWakeupLiveness` independently checks each retained, unstarted
invocation for a generation-matched `WF_RUN` message. The seeded 20-start
workload checks the gap before each repair scan and verifies that the scan
closes it. A skipped-reconciler control leaves a missing run that the checker
rejects; deleting a repaired run makes it fail again. Journal markers now
record the invocation sequence. A reused-ID control deliberately leaves the
old journal after purging its invocation and shows that the checker detects
the new generation's missing run even when the scanner mistakes the old
journal for the new one. Purging the old journal lets the scanner repair it.

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
hole; three repeats passed. A retained-state liveness checker now verifies
that every eligible retained signal has the scanner's stable `WF_RUN` message
ID. It names absent invocations, stale generations, consumed signals, and
terminal invocations. Every seed runs the checker after repair, and a
negative control detects a skipped scanner, a dropped publish, and a removed
retained wakeup. A second 1,000-seed workload interleaves a
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
signal loop remains outside the model. The worker's production
`DrainSignalsWithPort` now reads through a narrow signal/Object Store port
while retaining its generation, hash, and journal-append decisions. A fifth
1,000-seed workload drains 20 invocation fixtures per seed with unrelated
global stream messages, purged holes, stale generations, blob references,
lost signal publish acknowledgments, failed journal appends, corrupt hashes,
and replay from consumed journal records. Eligible signals are journaled once
and replay without another append; bad hashes fail before append. A three-node
contract compares subject-filtered next-message reads, a deleted sequence,
stale-generation skipping, ordered consumption, and journal replay against
the model; three repeats passed. The full worker handler still uses real
JetStream outside simulation for its lease, journal, and result decisions.
A sixth 1,000-seed workload connects production client signal publishing,
signal reconciliation, and worker signal drain against the shared model. It
sends 20 ordered signals per seed under lost publish acknowledgments and lost
or dropped wakeup enqueues, repairs uncertain wakeups, journals each signal
once, retries some keys after consumed-signal purge, rejects changed retry
payloads, and verifies replay plus a subsequent scan adds no wakeup. Seed 42
replays from disk and is byte-identical across processes. The drain now uses
production journal CAS for each `SignalConsumed` entry. One committed journal
append loses its acknowledgment at a seeded position; worker redelivery
reloads the retained journal and finishes without a duplicate entry. Every
retained sequence and payload matches the replay records. Dispatch, lease
fencing, and terminal result persistence are still separate modeled slices.

The signal liveness checker also runs after 1,000 seeded production-client
signal scenarios and before and after each seeded client-to-worker signal
pipeline drains 20 signals. The checker sees every enabled signal before
drain and no enabled signal after its journaled consumption. A further
1,000-seed workload runs the worker's production child-to-parent notification
decision through `Client.SignalToGeneration`, with distinct parent and child
invocations and a retained child terminal journal. It injects dropped or
unacknowledged signal publishes and wakeup enqueues, retries the same child
notification, and checks one generation-bound signal and one parent wakeup.
Reusing the parent ID fences the old child notification. The signal scanner
deduplicates a further repair pass, and the retained-state liveness checker
verifies the final state. Seed 42 is pinned and replays across processes.
This covers child notification after a terminal outcome; the worker handler's
terminal write and lease are still outside the model.

The timer repair slice runs production `TimerScan.Scan` through retained
invocation and journal reads and run enqueue on a narrow port. A 1,000-seed
workload scans 20 invocations per seed across due and future timer requests,
completed and terminal journals, unrelated steps, deleted invocation holes,
dry runs, duplicate scans, and lost or dropped wakeup acknowledgments. It
advances virtual time to make future requests due. Exact trace replay and
byte-identical cross-process traces pass. A three-node contract compares
model and real scan results and retained run counts for due, future,
completed, terminal, and deleted-invocation cases; three repeats passed.
Native timer routing under partition and the worker handler's timer replay
remain outside this scanner model.

The suspended-wait scanner now uses a narrow invocation, journal, filtered
signal-read, and wakeup port. A 1,000-seed workload runs production
`SuspendedScan.Scan` over 20 invocations per seed. It covers due and future
timers, timer/signal select, matching and stale-generation signals, consumed
and already-used signals, unrelated subjects, terminal journals, purged
invocation holes, dry runs, uncertain wakeup acknowledgments, deduplicated
rescans, a fresh wakeup ID in the next ten-second retry window for a
still-ready wait, and virtual time advancing past a future timer. Its trace replays
from disk and across processes. A three-node contract compares filtered
signal reads through a deleted sequence hole, scan candidates, and retained
wakeup counts with the model. A separate retained-state liveness checker
independently reads suspended journals, pending timer deadlines, available
signals, and reconciliation message IDs scoped to journal tail and retry
window. It names waits that remain
blocked and fails if an enabled wait lacks a retained `WF_RUN`. Every seeded
scanner schedule runs this check after virtual time advances. Future timers
report their exact virtual deadline plus grace.

Another 1,000 seeded schedules run the production suspended-wait scanner
inside the leased reconciler loop. Each seed mixes ten due timer waits with
ten matching signal waits, injects a dropped or unacknowledged wakeup
enqueue and cursor CAS, then replaces the scanner. The saved cursor resumes
the scan, all 20 wakeups remain unique, and traces replay from disk and
across processes. Every seed checks that all 20 enabled waits have retained
wakeups. A negative control skips the scanner, drops its first wakeup publish,
and later removes a retained wakeup; the checker detects each missing
message. Recovery scans and virtual time advancement clear the failures.
This is a suspended timer/signal I5 check; child completion and integrated
worker advancement still need coverage.

A separate 1,000-seed transport replay connects the production suspended
scanner to the modeled durable consumer. It delivers a ready signal wakeup
while a lease is held, drops the wakeup's `NakWithDelay` before commit, and
keeps the original delivery pending for the 20-second `AckWait`. The scanner
deduplicates within its first window, publishes a fresh run in the next
ten-second window, and the consumer fetches that run before the original
redelivery. Once the modeled journal becomes terminal, a rescan publishes
nothing; the old delivery is then acknowledged as a no-op. The seed chooses
the first delivery offset, and saved traces replay exactly. This isolates the
transport and scanner timing; it does not model a full signal handler or prove
which acknowledgment was lost in the real-cluster failures.

A further 1,000 seeded schedules interleave two live suspended scanners at
lease, cursor, cadence, invocation, journal, signal-read, and wakeup calls.
A seeded cursor write loss forces lease turnover while one wakeup request or
acknowledgment is lost. All ten waits, split between due timers and matching
signals, retain one wakeup each. The trace replays from disk and across
processes.

The worker's timer publication now uses a narrow port for native schedule
messages and fallback timer records. A 1,000-seed workload runs production
`ScheduleTimerWithPort` for 20 timers per seed, alternating native and fallback
backends and injecting dropped publishes, committed publishes with lost
acknowledgments, and duplicate retries. Virtual time delivers native targets
only after their due time; fallback records remain for the separate poller.
The trace replays exactly and is byte-identical across processes. A three-node
contract compares retained native headers, fallback payloads, duplicate
acknowledgments, hidden acknowledgments after commit, and one routed target
per native timer. Route partition timing remains outside this timer-publication
slice; the integrated worker timer workloads above exercise replay.

The native schedule model now has an explicit quorum cut: a target that becomes
due during the cut stays retained until heal and an optional seeded virtual
recovery delay. The integrated production-worker timer workload exercises a
12-second overdue target with 0, 1, 5, or 20 seconds of post-heal delivery
delay, then requires one completed workflow and a post-heal resume under
30 virtual seconds. A three-node contract publishes a real native schedule,
isolates one node and stops another before due time, then heals routes; the
target appears once with two live nodes and the restarted third node reads it. This
contracts eventual delivery, not a fixed NATS leader-recovery delay.
Seed 6 with a five-second virtual recovery delay is pinned in the regression
corpus; the previous timer trace retains its original workload version and
still replays byte-for-byte.

The fallback timer scanner now has a narrow retained-read, state-read,
wakeup-publish, and delete port. Another 1,000-seed pipeline publishes 20
fallback timers through the worker's production timer path and scans them
through production `FallbackTimerScan.Scan`. Due, future, purging, and
tombstoned generations are mixed. One wakeup publish loses its acknowledgment,
then one timer delete loses its acknowledgment; retries retain one wakeup per
eligible timer, remove retired records, and leave future timers until virtual
time advances. The trace replays exactly across processes. The three-node
timer contract now compares both failure cuts with a real stream, including
duplicate wakeup suppression and deletion after a hidden acknowledgment.

A separate 1,000-seed workload runs the same production fallback scanner
inside the leased production reconciler loop. Each seed schedules 20 due
fallback timers, injects one dropped or unacknowledged wakeup publish, one
dropped or unacknowledged timer delete, and one dropped or unacknowledged
cursor CAS. It then replaces the scanner. The persistent cursor advances,
all 20 wakeups remain unique, and no fallback timer remains. Traces replay
exactly, including from a saved file and across separate processes.

A further 1,000 seeded schedules interleave two live fallback scanners at
lease, cursor, cadence, timer read, wakeup publish, and delete calls. A seeded
cursor write loss forces lease turnover; a wakeup and a delete also lose
their requests or acknowledgments. All ten due timers publish one wakeup
and are removed. The trace replays from disk and across processes.

The shared reconciler loop now has a narrow port for lease acquisition and
renewal, cursor load and CAS save, and cadence waits. The model uses separate
KV transports for the expiring lease and durable cursor. A 1,000-seed workload
runs production `StartScan.Scan` or `TimerScan.Scan` inside the production loop for 20 invocations,
injects a dropped cursor write or a committed write with a lost acknowledgment
at a seeded save, stops the first scanner, and starts a replacement. It checks
one retained wakeup per invocation, exact trace replay, and byte-identical
traces across processes. The existing three-node cursor contract checks that
a replacement starts from the saved cursor and stale revision writes fail.
A second three-node contract hides an acknowledgment after real `WF_STATE`
cursor create or update commits, then confirms a reader on another node sees
the committed cursor, advances it, and rejects the old revision. Three
race-instrumented repeats passed. A further 1,000-seed workload runs two
live production start-scanner loops through cooperative yield points at lease,
cursor, cadence, and retained stream calls. A seeded cursor write loss forces
lease turnover. Every schedule retains ten distinct wakeups, and five
race-instrumented runs replay exactly. Native timer routing and the worker
handler remain outside this model.

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
run passed. This slice exercises the dispatch loop and consumer contract with
a callback handler. The integrated short-handler workload above exercises
production lease, journal, and result decisions. A second 1,000-seed workload now runs two production partition
loops against one modeled durable. It yields at creation, fetch, wait,
acknowledgment, nak, and progress calls; interleaves an injected leader-change
error, unacknowledged first deliveries, and final acknowledgments; and drains
all ten messages in every seed. Traces replay exactly and are byte-identical
across processes. It exposed a clean-shutdown gap: cancellation during
consumer creation returned `context canceled`; `RunPartition` now exits
normally when its context is already canceled. The real shared-durable
contract above confirms separate clients receive distinct messages and
redelivery survives client replacement. This dispatch-only workload uses a
callback; the integrated short-handler workload above runs `Worker.handle`.
Server-driven leader timing remains outside the modeled comparison.

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
FAULT_TRACE=/path/to/failing-trace.json FAULT_TRACE_MIN_OUT=/path/to/minimized.json go test ./sim -run '^TestMinimizeFaultTrace$' -count=1
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
is limited: separate integrated worker workloads cover short handlers,
signal resume, timer wakeups and eight-timer bursts, two-level child cascades, running cancellation, heartbeat handoff,
large result objects, and snapshot reads and writes. Combined blob and
retention faults, concurrent heartbeat and failure actor turns, Raft
elections, and disk storage remain outside the in-memory model. The journal's
five-second attempt
deadline still uses wall time; only CAS retry waits are virtual in this first
slice. A discrepancy seen only on real
NATS is a candidate model gap or environment/server issue, not proof of a
server defect. See the [Tier 1 plan](implementation-plan.md#distributed-verification)
for the remaining transport contract and invariant gates.

### Successive worker-kill delivery timing

`TestSuccessiveWorkerKillDeliveryRecovery` combines `DispatchTransport` with production lease acquisition and journal append/read code. It compares earlier 20s and 10s AckWait controls with the production default (lease TTL plus 1s), after a first kill before the heartbeat and a second targeted kill at 14s, 16s or 21s. Seeded effects take 1.8s, 2s or 2.2s. The default must finish below 30s across these nine cases, preserve one terminal journal, reject stale owners, and drain the modeled durable. Changing the default back to 20s fails the gate.

The version-2 workload pins `worker-successive-kills-v2.json`; the earlier version and its trace remain replayable. `SIM_SEEDS` controls the local/CI seed count, and `SIM_SUCCESSIVE_KILLS_OUT` exports seed 42. Exact and cross-process replay checks cover transport events. This is a sequential delivery/lease timing slice; it does not execute OS process kills or full worker handler/heartbeat goroutines, and does not prove arbitrary repeated-kill or long-effect bounds. Real process-kill matrix runs supply that separate evidence.

### Child completion liveness from retained outcomes

`CheckChildWakeupLiveness` starts with retained terminal child journals and their invocation parent headers. A current child/parent generation must have either a matching consumed outcome, a matching parent signal with its retained wakeup, or a terminal parent. It names absent/reused parents and nonterminal/stale child generations. Starting from the child journal catches a skipped notifier even when the signal-only checker sees no eligible signals. It also checks the notification payload (or blob-reference hash metadata), so another child's result cannot satisfy the obligation. Blob content validation remains in the production drain and object transport checks.

The seeded notification workload now checks the missing obligation before publishing, after injected lost/drop outcomes, and after notification retries. Existing pinned traces remain unchanged because these checks only read quiesced model state. Negative controls remove a wakeup, alter an inline outcome despite retaining matching hash metadata, and supply a wrong consumed outcome; positive cases cover consumed/purged signals, generation-matched terminal parents and obsolete parent/child generations. This extends child-completion I5 checker coverage; it does not add a full process-kill/scanner/worker pipeline.

### Metadata handle cache cancellation

`TestSeededHandleCacheCancellationReplay` runs the same `internal/handlecache.Cache` used by journal stream/state and snapshot state/object adapters. A metadata lookup remains held while two callers enter the contended cache wait. The model then delivers a virtual cancellation or deadline to one waiter before releasing the lookup. That waiter must return independently, the live waiter must obtain a handle, and a failed first lookup must be retried exactly once rather than cached. Successful handles are shared and cached; an already canceled caller cannot return cached success. Eight seeded cases combine lookup success/failure, waiter identity and cancellation/deadline.

The transcript records controlled actor boundaries; observing the wait context's `Done` establishes that the caller entered the wait before cancellation. Virtual time advances by one millisecond for that event. Host-time guards detect a stuck actor rather than drive the schedule. Exact replay, byte-identical cross-process traces and the seed-42 pin cover this cache slice. `SIM_HANDLE_CACHE_OUT` exports the pin and `SIM_SEEDS` controls the seed count. Journal adapter tests separately verify cancellation for all four cached resources and independence of state from blocked stream/object lookups; the former mutex implementation fails all six cases. This isolates a local cancellation defect without asserting a server cause for an entire mixed-workflow latency miss.

### Sequential signal-processing request cost

`TestSeededWorkerSignalWriteLatencyReplay` runs a production worker with 16 buffered signals against virtual lease, journal and signal transports. The seeded per-request delay is 0, 20, 170 or 300 ms. It counts 51 lease KV updates (one acquisition update and 50 renewals), 50 journal appends and 18 journal/signal reads before completion: 119 sequential delayed requests. The journal must retain 16 ordered consumptions, 50 entries, the expected sum and one immutable terminal outcome; the durable must drain. The measured costs are 0, 2.38, 20.23 and 35.70 seconds respectively.

The 300-ms case is explicitly recorded as `known_over_30s_control`, not a clean liveness schedule. This demonstrates that individually successful requests can exhaust the latency budget under the current write protocol. It does not reproduce a broker failure or establish why a real request was slow. Heartbeat ticks are held quiet to isolate per-entry renewal; other request classes have zero added latency, and the fixture starts with buffered signals rather than replaying a previously suspended prefix. These are explicit cost-model assumptions, not a complete bound on production recovery.

`SIM_SIGNAL_COST_OUT` exports seed 42 to the pinned `worker-signal-write-latency.json` corpus. `SIM_SEEDS` controls schedule count. Exact replay, cross-process trace identity and coverage of all four delays are checked. The 100,000-seed local cost-model run passed in 88.12 seconds; focused race/pinned checks and the full simulator suite passed. This verifies the model and its known failing control, not the release's clean-seed gate.


## Matching start retry repairs its enqueue

The seeded start transport scenario now requires a matching production `Start`
retry to repair a dropped first run enqueue before any reconciler scan. It checks
one retained invocation and one generation-specific run message, rejects changed
input, and verifies wakeup liveness immediately after retry. Hidden invocation
acknowledgments also take this repair path. The same seed and trace replay exactly
across processes; seed 42 is pinned as `start-retry-repair.json`. Restoring the old
start implementation fails seed 1's retained-run assertion. This models runtime
repair decisions, not the real consumer seed 14 scanner delay or NATS leadership.


## Repeated promise resolution

The production-worker child execution workload now awaits the same resolved
promise twice and mutates the first returned slice before the second await.
Every existing seeded fault choice must still produce the same child and parent
terminal bytes and one consumption of the result signal. It runs production
`AwaitPromise` decisions and exact/cross-process transport replay. The second
await introduces no durable operation, so the pinned child transcript stays
unchanged. This does not model object-cache exhaustion; focused SDK contracts
cover the sixteen-MiB bound and retryable object reads separately.


## General durable selection

`TestSeededSelectManyReplay` runs production timer creation, `wf.Select`,
`AwaitPromise` and journal append/read against the seeded journal transport.
It varies argument priority, readiness and a dropped or hidden committed
selection completion. Resume can introduce earlier ready cases; a retained
completion must still replay its recorded winner. A selected promise remains
reusable without consuming another signal. The final five-entry journal and
selected bytes are checked. Exact replay, cross-process identity and the
`select-many.json` regression pin cover this slice. A local 100,000-seed run
passed in 11.37 seconds, with focused race and pinned-corpus checks passing.

Buffered signals and promise outcomes are fixture inputs; timer readiness uses
virtual wakeup time. This slice does not simulate the full child notifier,
worker dispatch or suspended scanner pipeline. Real three-node restart tests
separately exercise signal, timer and actual child-promise winners, including
scanner repair after removal of the parent's retained run wakeup.


## Multi-case suspended repair and retained wakeup checks

`TestSeededSelectSuspendedScanReplay` runs production `SuspendedScan.Scan`
against the retained invocation, journal, signal and enqueue transport. Twenty
invocations per schedule vary due/future timers, current/stale child outcomes,
matching and unrelated signals, drained-but-unused and already-used signals,
terminal/no-journal invocations, invocation holes and dropped/hidden enqueue
acknowledgments. Multi-case requests combine a signal, promise and timer.
Dry runs must not publish; repair retries deduplicate immediately and create a
fresh wakeup in the later retry window. Virtual time enables future timers.

The independent retained-state I5 checker now understands multi-case waits.
Before each repair, it rejects every enabled selection lacking a wakeup and
names blocked waits. After uncertainty, it distinguishes an absent dropped
publish from a retained publish whose acknowledgment was hidden. Final retained
wakeups must satisfy all enabled selections. Seeds 1 and 42 pin promise repair,
stale outcomes, used signals, lost acknowledgments and dropped publishes;
exact and cross-process replay preserve the transport transcript. Existing
legacy scanner workloads and pins are unchanged. This slice exercises scanner
and retained-state decisions, not the full worker/notifier pipeline or Raft.

The local 100,000-seed scanner slice passed in 220.32 seconds. Final focused
race/pinned checks passed in 33.16 seconds and the full simulator suite passed
in 60.61 seconds. These counts cover the modeled scanner decisions only.


## Automatic membership and assignment takeover

`TestSeededMembershipReplay` runs production registration, membership listing,
coordinator election, `Controller.Step`, balanced planning and assignment CAS
against virtual KV transports. A starts with all 64 partitions; B joins. The
seed chooses a clean rebalance, a dropped assignment update, or a committed
update with its acknowledgment hidden. A then stops without releasing its
registration or coordinator lease. B renews at three-second virtual intervals
and takes over at the twelve-second expiry boundary. Every partition must end
with B; A's next step must fail with `lease.ErrLost`, its cleanup must preserve
B's coordinator lease, and another stable pass must write no assignments.

Membership and assignment storage are separate models, with TTL only on the
membership bucket. The production real adapter and modeled adapter share the
same membership read/lease port and controller pass. The periodic runner wraps
that pass with its bounded context and wall-clock ticker. A local 100,000-seed
run passed in 66.27 seconds, with exact/cross-process replay and a seed-42 pin
covering the hidden assignment acknowledgment. Focused race/pinned checks
passed in 12.15 seconds. This is a sequential controller/transport slice with
three assignment-fault choices and fixed heartbeat phase; it does not model
arbitrary concurrent controllers, OS kills, worker handlers or server Raft.

A separate real three-node contract SIGKILLs the actual A worker/coordinator
inside its first durable effect, drives 64 other ten-step workflows, and checks
B's complete assignment takeover, higher workflow epoch, all 65 immutable
results, client histories and retained-state integrity. That fixture supplies
the independent worker/process evidence rather than inferring it from virtual
KV expiry.


## Mixed-version WorkQueue retention reproduction

`TestMixedVersionExplicitAckWorkQueueRetention` isolates the partial-upgrade
queue discrepancy using JetStream publish, fetch and explicit ack operations,
with no workflow execution. Three NATS 2.11.17 servers create a replicated
WorkQueue and durable, deliver 33 records, upgrade the consumer leader to the
module-pinned 2.15.0 server, and acknowledge even indices before odd indices.
An all-old control removes every message. Moving both stream and consumer
leaders onto the upgraded node before ack also removes every message.

Keeping the stream leader on an old node leaves all 33 records readable while
the consumer reports ack floor 33, zero pending and zero ack-pending. Both
ordinary Ack and confirmed DoubleAck exhibit this behavior. The inherited
consumer-replica configuration used by the runtime reproduces it too, with
three actual peers and current replicas. Queue diagnostics record raw messages,
metadata, last scanned sequence and whether the bounded scan completed.

`TestSeededWorkQueueRetentionReplay` now separates committed consumer progress
from physical stream retention in `DispatchTransport`. Each schedule delivers
33 messages, chooses an acknowledgment order, and selects ordinary removal,
held removal, or held removal combined with a lost committed ack reply. Held
records remain stored after thirty seconds while the consumer reports no
pending work and does not redeliver them. Duplicate confirmed acknowledgments
must not silently complete removal. The stream-level `CheckDrained` rejects
this state even though the consumer has drained; seed 5 pins that failure in
`workqueue-retention-held.json`.

The workload also tests explicit deferred removal completions, including
rejection before ack and idempotence afterward. These completions are model
controls, not an inferred repair mechanism for real mixed-version NATS. The
held modes assert an expected liveness failure; passing the test means the
checker recognizes it, not that runtime liveness under this fault is clean.
The local 100,000-seed run passed in 24.05 seconds, the complete simulator suite
passed in 53.74 seconds, and the final pinned race corpus passed in 1.96 seconds.
Exact replay and byte-identical cross-process traces cover this transport slice.

The real conformance failure remains a release gap. The observed placement
control does not establish the internal compatibility cause or a general
runtime repair protocol. The full rolling-upgrade matrix retains seeded node
order.


### Observed post-ack recovery and likely proposal-ownership mismatch

The real `mixed-move-after-ack` control now uses the inherited replica
configuration and requires all 33 acknowledged records to remain stored for
thirty seconds before moving the stream leader to the upgraded consumer leader.
It then requires zero stored messages and unchanged completed consumer progress.
The final race test passed in 40.57 seconds, with all 33 raw pre-move records
saved and a complete sequence scan. Afterward every original sequence must
return message-not-found, independently of the zero stream count. This supports explicit modeled retention completion
for that fixture; the simulator does not implement server leader election or
infer universal recovery from a move.

Source inspection supplies a likely explanation for the retained placement:
`stream.ackMsg` in NATS 2.11.17 allows the consumer leader to propose removal,
while 2.15.0 allows the stream leader. With an old stream leader and upgraded
consumer leader on different nodes, each node fails its own ownership condition.
This is a source-supported causal hypothesis, not an instrumented proof that
those branches were taken in every real failure. See the pinned upstream
[2.11.17 implementation](https://github.com/nats-io/nats-server/blob/v2.11.17/server/stream.go#L6574)
and [2.15.0 implementation](https://github.com/nats-io/nats-server/blob/v2.15.0/server/stream.go#L9372).
The strict split-version conformance cases and full seeded rolling-upgrade
matrix remain independent requirements.


### Two real consumer groups and distinct upgraded leaders

`TestMixedVersionMultipleConsumerRetentionRecovery` extends the real placement
controls to two independent R3 consumer groups with inherited replicas. Each
receives 33 messages on its own filter while all three servers are old. One
consumer leader upgrades. Interleaved out-of-order DoubleAck calls commit both
groups, but after thirty seconds only the upgraded leader's 33 records remain
stored under the old stream leader; all 33 old leader records are already absent.
A complete raw scan and individual sequence reads verify the distinction.

Moving the stream leader onto the upgraded node removes the retained group.
After upgrading the other consumer leader, a second 66-message round drains
with two distinct upgraded consumer leaders and an upgraded stream leader. All
132 original sequences must be absent and each consumer's ack floor must match
its delivered count. The initial race proof passed in 44.99 seconds. This is
real transport evidence across multiple consumer groups, not a new simulated
Raft model or a general rolling-upgrade repair algorithm.

The dedicated `mixed-version-retention-controls` workflow runs old-only,
pre-ack co-location, post-ack leader-move and multi-consumer controls. It saves
pre-move raw state even on success. Expected retained-state recognition is
explicitly separate from the strict split-version conformance tests that still
fail and the sustained runtime matrix.


## Cancellable waits behind lease transport operations

`TestSeededLeaseGateCancellationReplay` holds a production `Lease.Renew` at
its KV update boundary while another caller enters Renew, Release or Cleanup.
A virtual cancellation or deadline must return that waiter before the held
renewal completes, without making any KV request or poisoning the lease.
After the held renewal succeeds, a fresh renewal must succeed; after its
uncertain transport failure, ownership must remain lost. Twelve seeded cases
combine waiter operation, cancellation kind and held-renewal outcome.

Production lease revision decisions now use a cancellable serialization gate.
The original mutex held across network calls prevented waiting callers from
honoring their contexts; restoring that implementation makes seed 1 fail at
the controlled waiting boundary. The model observes the wait through the
context's Done method, delivers cancellation at one millisecond of virtual
time, and uses host guards only to detect stuck actors. It does not simulate
NATS server delays or prove the complete mixed-fault latency budget.

The local 100,000-seed workload passed in 4.08 seconds. Exact replay,
cross-process trace identity, the `lease-gate-cancellation.json` pin and the
complete pinned race corpus cover this slice. The full simulator suite passed
in 49.97 seconds and the lease suite in 1.16 seconds; focused race checks passed.
A live seed-2 mixed-fault race proof passed at 23.42-second terminal p99, but the
earlier clean-runner 63.13-second miss is not attributed to this defect alone.


## Shared retained-journal validation

`CheckSnapshot` and the real stream audit now validate logical journal stream
sequences through the same checker: sequences must be positive and increasing,
while global sequence holes are allowed. Unknown entry kinds and a successful
terminal with an unresolved request fail. Failed terminals may retain pending
requests, and nonterminal pending or suspended histories remain valid cuts.
Negative controls demonstrated that all five corruptions survived the former
snapshot checker; real three-node controls independently cover unknown kinds,
unresolved success and a valid failed pending step. These controls strengthen
I2/I3 checks without claiming the complete six-mutation release campaign.


## Manual dead-owner terminal drain

`TestSeededDeadOwnerDrainReplay` runs production balanced assignment CAS and
modeled durable delivery over virtual KV/dispatch. It records an expected stall
when a manual membership list retains a dead owner, then requires live-owner
correction, redelivery two and confirmed queue removal. Seeds vary the retained
partition and dropped or hidden committed assignment replies. Removing the
correction fails the owner check; seed 42 is pinned in `manual-owner-drain.json`.
This models the recovery obligation of the real combined rebalance fixture,
whose final assignments previously could point at a killed subprocess. It does
not model handler execution, process pauses or the automatic membership runner.


## Lease cleanup after a concurrent revision change

`TestSeededCleanupConflictReplay` runs production renewal and cleanup against
virtual KV. A hidden committed renewal fences the owner; at cleanup's delete
boundary another same-owner update or successor acquisition changes the read
revision. Cleanup must reread worker/epoch identity, remove only its own lease,
and preserve successor bytes and revision. Three persistent conflicts must
return a retryable revision error rather than presumed successor ownership.
Restoring the previous implementation fails seed 1. Exact/cross-process replay,
100,000 schedules and the `lease-cleanup-conflict.json` seed-42 pin cover this
slice. Real three-node controlled CAS races supply separate transport evidence.
These controls isolate a local cleanup decision; they do not simulate disk
stalls or clear the failed seed-12 mixed latency gate.


## Older lease values during cleanup

`TestSeededCleanupStaleReadReplay` delivers one or three stale pre-initialization
KV values after the lease has acknowledged its epoch update and a dropped renewal
has fenced execution. Cleanup must reject their older revisions as ownership
evidence, converge on a fresh own value or return a bounded revision error, and
never restore execution. Restoring the prior implementation fails seed 1.
Exact/cross-process replay, the 100,000-seed pass and seed-42
`lease-cleanup-stale-read.json` pin cover the decision. Real three-node controls
also supply an earlier-worker value and preserve fresh successor state. This
slice does not make a not-found reply authoritative or clear mixed-fault latency.


## Virtual worker operation timing

The `worker_signal_write_latency` workload attaches the optional production worker
operation observer with a scheduler-backed observation clock. It checks call
identity, outcomes, append indices/kinds, all 121 events and the exact duration
of the 119 delayed calls. Observation does not advance virtual time or alter the
transport transcript. Exact/cross-process replay and the existing pin remain
valid; 100,000 schedules passed in 68.31 seconds. The deliberately over-30-second
control remains a failing latency classification, not a relaxed recovery gate.

For a real mixed-fault run, set `FAULT_SCHEDULE_OUT` to preserve the adjacent
`-operations.json` artifact. Each event ends at `At` and its call starts at
`At - Duration`. Concurrent heartbeat calls can overlap execution calls, so
summing all real durations is not elapsed invocation time. Durations include
client waiting/retries and do not identify a server-side cause. The coverage and
real-run evidence are recorded in the implementation status.


## Filtered consumer creation failures

`journal_open_failure` drives production long-read creation decisions through
one virtual deadline or no-responder outcome, three persistent deadlines, or a
semantic config rejection. Attempts retain the same sequence after the 64-entry
serial prefix. It verifies all reconstructed records, bounded attempt counts and
virtual elapsed time, and fails against the earlier fail-on-first-error code.
The real adapter separately bounds requests to three seconds; a TCP-proxy
contract holds an actual creation reply until the retry and verifies unchanged
journal contents. The seed-42 pin, exact/cross-process replay and 100,000-seed
pass cover this slice. The model supplies named replies rather than simulating
the NATS metadata controller that caused the original CI read to wait.


## Heartbeat renewal reuse

`lease_heartbeat_reuse` checks production lease decisions for recent acknowledged
renewals, idle interval expiry, delayed replies, takeover after a pause, lost
ownership, backward clocks and canceled callers. Freshness uses request start,
not acknowledgement receipt. An overlay that moves the timestamp to receipt
fails seed 3; append-facing renewal remains unconditional. The integrated worker
heartbeat workload counts the actual virtual KV calls for two progress ticks;
restoring the previous worker fails seed 15 with a redundant second update.
Both workloads passed 100,000 seeds with exact/cross-process replay. Pins retain
the delayed-acknowledgement, lost-owner and integrated two-heartbeat cases.
These controls prove transport-call reduction for that virtual interleaving,
not a general explanation of observed NATS request delay or a mixed p99 pass.
