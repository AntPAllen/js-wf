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
or closed tick source cancels the first blocked effect. It naks the retained
run message and cleans up its lease. The successor receives redelivery,
replays `StepRequested` under a higher epoch, executes the effect once more,
and writes the only terminal result. I1/I2/I3/I6, exact replay, cross-process
traces, and the race detector pass. Pinned traces cover a hidden renewal
reply and a failed progress write. A three-node worker-level fixture hides
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
leased fallback poller and simultaneous timer/signal choices remain outside
this integrated workload.

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
rescans, and virtual time advancing past a future timer. Its trace replays
from disk and across processes. A three-node contract compares filtered
signal reads through a deleted sequence hole, scan candidates, and retained
wakeup counts with the model. A separate retained-state liveness checker
independently reads suspended journals, pending timer deadlines, available
signals, and stable reconciliation message IDs. It names waits that remain
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
signal resume, timer wakeups, running cancellation, heartbeat handoff,
large result objects, and snapshot reads and writes. Combined blob and
retention faults, concurrent heartbeat and failure actor turns, Raft
elections, and disk storage remain outside the in-memory model. The journal's
five-second attempt
deadline still uses wall time; only CAS retry waits are virtual in this first
slice. A discrepancy seen only on real
NATS is a candidate model gap or environment/server issue, not proof of a
server defect. See the [Tier 1 plan](implementation-plan.md#distributed-verification)
for the remaining transport contract and invariant gates.
