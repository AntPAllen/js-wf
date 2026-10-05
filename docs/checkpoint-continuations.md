# Materialized SDK checkpoints with named continuations

Status: continuation contract, SDK cursor foundation, bounded frame codec and
SDK capture/restore, archive/runtime publication and prefix-free journal reads.
SDK Continue publication and worker stage dispatch, suffix resume, boundary
compaction and handoff are implemented. Full crash/restart and retirement gates remain open. Complete-history offline
stage replay is implemented with focused/model/real-journal checks below.
This document preserves the plan's checkpoint requirement; it does not count
as a completed checkpoint feature.

## Execution contract

An ordinary Go handler starts at its entry point on replay. Loading its final
state at that point cannot recreate its earlier stack, local variables or
control flow. It also changes earlier `GetState` observations. Checkpointed
workflows therefore opt into registered, named continuation functions. Each
boundary serializes the local data needed by the next function. Existing
ordinary handlers retain their full replay contract.

Registration contains an initial handler and a map of stable,
versioned continuation names. `wf.Continue(ctx, nextStage, data)`
commits a checkpoint and ends the current delivery. Its caller must return the
continuation result immediately. The worker invokes the named next function
with the original invocation input and serialized continuation data. It creates
no second invocation and does not reset the journal, fencing epoch or limits.
Use `worker.WithContinuations(type, stages)` with the existing initial handler.
Each `worker.ContinuationHandler` receives `(ctx, originalInput, locals)`.
Registration copies the stage map at construction. Workers with an incomplete
registry reject a retained unknown stage before user code and retry the delivery;
the stage must remain deployed while retained checkpoints reference it. Ordinary
workflow contexts still return ErrContinuationUnsupported.

Changing a stage's implementation still requires ordinary replay/version
compatibility within that stage. Incompatible boundaries get a new name, and
old names remain registered for retained checkpoints and full offline replay.
An unknown stage or unsupported checkpoint format fails before executing effects.

## Frame and identity

A versioned, size-bounded checkpoint frame contains:

- Workflow type, ID and invocation stream sequence.
- Versioned next-stage name and JSON continuation data.
- Materialized SDK state values, consumed signal identities, buffered unconsumed
  signals with their retained-read cursor, and resolved promise
  outcome payloads, including content references rather than unbounded result caches.
- Absolute SDK entry position after the checkpoint's request/completion pair.
- Journal logical index/epoch for its anchor, with actual stream sequence carried
  by the manifest after the completion acknowledgment is confirmed.
- Required runtime prefix facts: panic-attempt count and timer cancellation facts
  needed to preserve fencing, retry limits and canceled-wakeup observations.

The SDK cursor and journal index are different counters. `Suspended`,
`SignalConsumed` and `Attempt` records can advance the journal without advancing
SDK request/completion position. Both counters must retain their original
meaning. A checkpoint never resets the 100,000-entry journal limit.

The cursor foundation adds a private SDK offset. Relative `position` still
indexes the supplied suffix; absolute position determines child IDs, timer
steps and `RunOnce` external keys. New ordinary contexts keep offset zero.
Continuation workers supply the saved offset while ordinary workers use zero.

Capture requires a fully consumed SDK boundary, no pending requested step and
no suspended wait. A live `TimerHandle` is tied to its original context and may
not cross the boundary; pending handles must be awaited or canceled first.
Continuation data must not silently serialize a context or timer handle as an
empty object. Promises can cross only with their explicit serialized identity
and the outcome/consumption state needed for reusable resolved awaits. Boundary
validation occurs before publishing a checkpoint request.

## Durable publication

The checkpoint uses the normal request/completion protocol. Its frame lives in
a content-addressed result object, and the completion references that object
with a hash. This keeps frames reachable by existing retained-journal reference
checks. Always spilling the checkpoint result makes the fast path independent
of a large journal prefix object.

The required ordering under the invocation lease is:

1. Append the checkpoint request using unconditional renewal and journal CAS.
2. Write and verify the bounded frame object, including its absolute SDK cursor.
3. Append/confirm the completion containing the frame reference and hash.
4. Archive the logical prefix using the existing full-journal snapshot format.
5. Publish one snapshot manifest with both archive and runtime-frame information,
   using revision CAS and monotonic checkpoint-anchor validation.
6. Purge only the confirmed archived prefix, keeping the checkpoint anchor in
   the retained suffix, then append the continuation suspension.
7. Publish a generation-scoped, deduplicated handoff run before release/ack.

The runtime pointer belongs in the existing snapshot manifest, not an unrelated
key that retirement could overlook. Retention must remove it with that manifest
and preserve the original journal-before-invocation retirement order. Full
logical journal reconstruction, invariant checking, visibility rebuild and
offline replay continue to use the archived prefix plus live suffix.

For checkpointed handlers, compaction occurs at continuation boundaries. The
checkpoint anchor remains among the retained entries. Generic compaction during
a later segment must not purge the only anchor for the current runtime frame.
Ordinary handlers keep their existing compaction behavior.

## Fast resume and failure behavior

Fast resume validates invocation generation, format, stage registration, object
hash, anchor identity and suffix continuity. It restores materialized state and
SDK position, then invokes only the named continuation. It does not execute the
initial handler or load the full archived journal object. Original invocation
input/hash validation remains required.

An absent checkpoint uses initial full replay. A frame from a retired generation
must never initialize its successor. Corrupt or inconsistent current-generation
metadata fails closed; it cannot become an empty initial state. Manifest revision
conflicts require an ownership/current-pointer reread, and an older checkpoint
may never overwrite a newer anchor.

A completion with an unpublished manifest is recovered by replaying the current
segment to the already committed checkpoint, then publishing its manifest. The
same effects do not rerun once their completions are recorded. A frame published
with an uncertain acknowledgment is reread and compared. Purge cannot start until
both frame and archive are verified and their manifest is confirmed.

The handoff is a two-write liveness boundary. The reconciler must recognize a
committed checkpoint/continuation suspension with no progress and re-enqueue
it using a fresh deduplication window. It must also repair the crash between
completion and manifest publication by waking the old segment. It stops after
the journal advances or the invocation retires. Checkpoint commit is an enabling
event for terminal and next-entry latency checks.

## Required verification before declaring completion

1. **Prefix avoidance:** thousands of state updates and multiple checkpoints;
   restart a worker and prove that earlier handler functions and prefix-object
   reads do not occur on the normal fast path. Results and state observations
   must equal a full-replay control.
2. **Identity:** child IDs, timer steps and external deduplication keys match the
   uninterrupted control after every cut. Losing the offset must be detected.
3. **Crash cuts:** every publication, manifest, purge, suspension and handoff cut,
   including committed-but-hidden acknowledgments, CAS conflicts and stale reads.
   No missing effects, duplicate terminal outcome or permanently absent wakeup.
4. **Generation and code:** purge/reuse, stale frames, unknown versions/stages,
   incompatible continuation data and every hash/index/epoch corruption fail
   before effects or reject the retired generation as appropriate.
5. **SDK semantics:** state reads/writes, reusable promises, signals, cancellation,
   timer boundaries, versioning, panic retry counts and journal-limit reservation.
6. **Audit:** full logical reconstruction and offline continuation replay match
   the original journal across compaction; referenced frame blobs survive sweep
   and become reclaimable after retirement.
7. **Tier 1:** production frame/manifest/dispatch decisions with seeded fault
   replies, exact replay, pinned negative controls and 100,000 clean seeds.
8. **Real cluster:** matching three-node transport contracts, worker SIGKILL and
   leader/route faults with integrity, immutable cross-peer results and liveness
   gates. Independent final-source matrix and soak requirements still apply.

The current cursor test proves only item 2's local SDK identity foundation:
after omitting a completed prefix, the same suffix declarations and identities
are produced; replay adds no effect, and losing the base is nondeterministic.
Durable frame storage and worker dispatch now have focused proofs below.
The full integrated verification above remains the acceptance target.

## Implemented frame codec

`internal/checkpoint` defines version 1, with a 16 MiB cap on the whole
serialized frame. Encode validates the frame and hashes exact stored bytes;
Decode verifies size/hash before JSON parsing, rejects unknown fields and
trailing JSON, checks semantic identities and binds generation/anchor to the
expected manifest values. No partial frame is returned on any error. Promise
outcomes preserve object references rather than derived result caches. State
and user locals decoded from the frame are detached from transport buffers.

The race suite and vet pass, with [raw proof and scope](scale/checkpoint-frame-2026-09-30/).
The SDK capture/restore functions now call the codec and enforce boundary and
live-handle rules. Continuation workers call them on the fast resume path. The worker must check
stage registration, actual
journal anchor, suffix continuity and configured journal limits before effects.
This does not advance the checkpoint feature to complete.

## Implemented SDK capture and restore

`Context.CaptureCheckpoint` validates the boundary and serialized locals, then
encodes a prospective frame whose SDK position includes the checkpoint pair.
It makes no durable write and never advances the context. `NewCheckpointContext`
verifies that frame against `CheckpointLocation`, validates ordered SDK suffix
entries after the anchor, and restores detached state, consumption identities
and promise outcome payloads. Derived result caches start empty; referenced
results pass the existing outcome hash verifier on their first restored await.
`CheckpointInfo` carries the stage, locals, panic attempts and canceled timer
facts for future worker dispatch and wakeup filtering.

Timer creation now registers handles in its context. Capture refuses any live
handle, including one whose signal selection won without cancellation. Contexts
and handles nested in locals are rejected before JSON serialization can turn
them into empty objects. Cyclic locals fail without an append. Capture/restore
controls and the full SDK/simulator checks are retained in
[SDK checkpoint proof](scale/checkpoint-state-2026-09-30/).

Ordinary workers replay from the initial handler. Opted-in continuation workers
use these primitives for suffix replay and named-stage dispatch. Frame hash/anchor binding is not a substitute for
checking the actual retained checkpoint completion and contiguous journal suffix.

## Implemented archive/runtime publication

`journal.WriteCheckpointSnapshot` verifies an already stored frame and committed
checkpoint pair, archives the prefix strictly before its completion anchor, and
publishes an archive/runtime manifest with revision CAS. The frame hash is bound
to the completion and its serialized data hash to the checkpoint request. SDK
positions are counted independently of auxiliary journal entries. Purge remains
a separate confirmed step and preserves the live completion anchor. Newer runtime
pointers reject older replacement attempts; generic compaction cannot remove
the anchor. `wf.Continue` and opted-in worker publication are wired.

Continuation manifests use version 2. Ordinary manifests stay version 1. This
lets old journal readers reject the new format before treating a continuation
journal as an ordinary replay/compaction target. Version 2 without a pointer and
version 1 with a pointer are rejected. The quiescent collector verifies the
current frame and marks its promise outcome references as well as the archive.

[Direct-storage real/model proofs](scale/checkpoint-manifest-2026-10-01/) include
100,000 local fault schedules, exact replay, two real checkpoint boundaries and
a real production-collector negative control. They do not prove worker dispatch,
frame creation cuts, suffix-only resume, online collection or complete retirement.

## Implemented prefix-free resume reader

`journal.ReadCheckpoint` verifies frame and retained completion, then returns
only the post-anchor records. Archive objects are not fetched. Generation or
metadata corruption never becomes an absent checkpoint. Named transport retries
preserve cursors; whole-read retries repair a newer checkpoint purging an older
anchor. The final manifest check rejects a superseded runtime pointer. Long
suffixes use filtered batch reads with the saved logical-index base. Ordinary
full reads keep base zero and continue supporting archive-based audit/replay.

[Reader proofs](scale/checkpoint-read-2026-10-01/) include 100,000 seeded schedules,
a pinned batch-offset mutation control and a real port that denies every archive
read while SDK state and 150 recorded effects replay correctly. Opted-in workers now use this reader. Stage dispatch must reject unknown registrations
before running code, restore runtime cancellation/attempt facts, derive append
indices from the anchor plus suffix, and preserve all lease/terminal guards.
The required worker restart/prefix-handler avoidance proof remains open.

## Implemented Continue step protocol

`wf.Continue` validates registered stage/locals, captures a frame for the
prospective completion, always stores and verifies its content-addressed object,
and appends a reference completion. A successful call ends the delivery with
ErrContinuation. The caller must return that error immediately; CheckComplete
cannot treat the pending continuation as a terminal success. A publication
error blocks that context's later SDK appends and normal completion.

Recorded completion replay obtains its historical index/epoch/attempt facts
from the caller's anchor resolver, verifies declared inputs and rebuilt frame
bytes, and loads the saved frame without writing another. Pending requests can
complete under a newer epoch. Locals are marshaled once and the object transport
receives a detached input buffer. `Context.Continuation` returns scalar metadata
for future worker manifest/handoff processing, not proof of a published manifest.

[SDK protocol proof](scale/continuation-sdk-2026-10-01/) includes all request/frame/
completion cuts, 100,000 seeded schedules and a lease-fenced real transport
contract. Worker code configures the resolver with historical anchor epoch, panic count
and signal cursor; later attempts and signals cannot change an existing frame.
Offline stage replay is implemented below; worker process-kill proof remains open.


## Implemented worker execution

`worker.WithContinuations` opts a type into immutable stage registration and
compaction at continuation boundaries. An absent checkpoint uses full initial
replay. A verified current-generation checkpoint restores state, buffered signals,
promise outcomes, cancellation facts and SDK position, then dispatches its named
stage with original input and explicit locals. Journal appends use the saved
anchor plus suffix length and retain the original global entry limit and terminal
slot reservation. Panic counts include the frame baseline and later attempts.
Every append still renews the invocation lease unconditionally.

The worker publishes the archive/runtime manifest, purges its confirmed prefix,
appends a continuation suspension and enqueues a generation/anchor-scoped handoff
before release/ack. It renews ownership before publication, purge and handoff.
Generic after-delivery compaction is disabled for opted-in types. Cancellation
still wins before handoff; an ignored unconfirmed Continue cannot become a normal
result or panic retry. A verified checkpoint ends the delivery even if the caller
ignores its return value.

Suspended repair recognizes both a completed checkpoint pair before manifest/
suspension publication and a continuation suspension. A later journal step stops
that boundary's repair. The existing retry-window message ID prevents a lost
handoff from becoming permanently suppressed by an older deduplication window.

A three-node race contract crosses two checkpoints after 1,000 state updates,
preserves buffered signals through signal purge, rejects an unknown deployed
stage before effects, and resumes on a replacement worker with archive reads
denied. Initial and middle handlers each run once; recorded prefix/suffix effects
run once and retain distinct absolute keys. This is a graceful worker replacement,
not SIGKILL or a completed fault-matrix proof. Seeded integrated transport checks
cover publication and handoff faults; [proof and remaining scope](scale/continuation-worker-2026-10-01/).


## Implemented offline multistage replay

`wf.ReplayWithContinuations` accepts the initial function, typed named-stage
callbacks and ordinary ReplayOptions containing invocation identity and object
bytes. It checks the complete logical journal structure with the invariant checker,
rejects unknown recorded stage registrations before calling user code, and
replays declarations from the initial handler. Each completed boundary rebuilds
and compares its frame using historical completion epoch, panic count and signal
cursor; it then restores the verified frame and remaining SDK suffix before
calling the next stage. Later signals/attempts cannot change an earlier frame.
No effect callback, child start, timer publication or journal/object write runs.

A history ending at checkpoint completion or its continuation suspension returns
ErrContinuation without entering the unrecorded next stage. An incomplete
checkpoint returns ErrReplayPendingStep. Observation reports the current stage,
verified boundary count and absolute played/recorded SDK positions. Missing or
corrupt objects, divergent locals/rebuilt state, malformed journals and stage
panics remain visible. A Completed history also checks the returned value against
its generation-bound terminal outcome and verifies any terminal result object.
RawMessage uses the worker's exact result bytes; other return types use JSON.
The original `wf.Replay` contract remains available for ordinary handlers.

For existing worker handlers, adapt the registered functions with closures that
supply the original input:

```go
result, err := wf.ReplayWithContinuations(history,
    func(c *wf.Context) (json.RawMessage, error) { return initial(c, input) },
    map[string]wf.ReplayContinuation[json.RawMessage]{
        "next_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
            return next(c, input, locals)
        },
    },
    wf.ReplayOptions{Type: typ, ID: id, InvSeq: generation, Objects: objects},
)
```

Objects must include referenced frames, signal payloads, step/promise results
and terminal results. `history` is the full archive-plus-live logical journal,
not only ReadCheckpoint's suffix. Failed/pending histories expose the replayed
error/wait; this API does not reenact every historical worker delivery, external
cancellation or panic attempt. Those integrated acceptance checks remain open.

[Offline replay proof](scale/continuation-replay-2026-10-01/) includes controls across two boundaries
with historical signal/attempt facts, a mutation that conceals rebuilt-state defects
by trusting stored frames, terminal mismatch/object controls, all seventeen
seeded worker publication modes and complete archived-journal replay after the
real three-node worker replacement. It does not close worker SIGKILL, retirement/
reuse, integrated runtime-semantics or the independent matrix/soak gates.


## Implemented worker SIGKILL publication cuts

Four real R3 three-node contracts now kill the worker process before manifest
creation and after confirmed manifest, journal purge and signal purge. Retained
journal/signal counts attest each cut. Repair must enqueue exactly the committed
checkpoint candidate; a successor pinned to another node acquires a higher
epoch after lease expiry. Before the manifest, it replays to the recorded
checkpoint without rerunning the prefix effect. After the manifest, it restores
one frame with all archived-prefix accesses forbidden. The signal-purge cut
also proves that buffered state survives an empty WF_SIG stream.

All cases pass immutable cross-peer results, full retained-state integrity,
offline history replay with no effects, and a settled repair scan with no new
wakeup. Recovery measured 12.93–13.13 seconds under race. Disabling production
completed-checkpoint repair makes the compiled before-manifest kill contract
fail. [Raw proof and scope](scale/continuation-kill-2026-10-01/) are retained.
These four actual process cuts strengthen the prior graceful replacement proof;
remaining publication cuts, combinations and the complete acceptance gates above
remain open.


## Implemented quiescent retirement and generation reuse contract

A real R3 worker contract now retires one of two completed checkpointed
invocations, removes its runtime manifest, and collects its old frame/archive.
The other invocation retains its frame/archive and a shared large result, whose
full bytes are checked before and after reuse. Every sweep occurs with workers
stopped. The same ID starts a higher generation; an injected predecessor manifest
is rejected by the reader and worker before user code or old object access.
After revision-CAS removal of that injected metadata, the fresh invocation
materializes new state/frame and completes, with immutable cross-peer results
and raw-state integrity. The survivor remains unchanged.

The final race contract passed in 19.385 seconds. A compiled mutation skipping
manifest deletion fails at the retired-pointer assertion. [Logs and scope](scale/continuation-retirement-2026-10-01/)
are retained. This focused lifecycle proof covers shared archive references;
retirement fault combinations, modeled continuation GC, frame-held promise
retirement and other acceptance gates remain open. Online GC remains unsupported.

## Continuation panic budget across two checkpoints and worker replacement

A real R3 three-node contract consumes one panic in the initial handler and one
in middle_v1, then checkpoints finish_v1 with PanicAttempts=2. After a signal
suspension, a replacement worker pinned to another peer restores state/locals
20 through an archive-denying port. Its final poison panic reaches the shared
three-attempt limit immediately. The full logical journal contains Attempt
counts 1, 2, 3 and Failed; initial/middle handlers each ran twice and replacement
entered the final stage only once. All peers return the same failure and the
raw audit finds one invocation and one terminal. Archive reads are zero.

The race contract passed in 20.925 seconds. A compiled production overlay resets
the saved panic baseline to zero; the contract fails before the final stage
with invalid step protocol in 5.708 seconds. Vet passed. [Logs, mutation and
source hashes](scale/continuation-panic-2026-10-01/) bind this proof. Runtime code
is unchanged from effe9c8.

Panic Attempt-to-Failed SIGKILL/reply-loss cuts, seeded integrated panic-budget
coverage and remaining continuation SDK acceptance gates remain open. This is
not final-source full-matrix or soak evidence; online GC and mixed seed 65's
latency miss remain open. The original million-timer runner is still live.

## Seeded continuation panic budgets and uncertain terminal writes

The production client/worker/journal/SDK now run a continuation_panic Tier 1
workload with one or two panics before each of two checkpoints and an invocation
budget of three to five. A fresh worker restores the exact panic baseline and
state/locals 20 without archive reads. Its enabled final panic must produce
contiguous Attempt counts and the same immutable failure, with a passing raw
retained-state audit and no earlier-stage entries after replacement.

Nine modes exercise clean execution plus drop-before-commit/hidden committed
ACKs for final Attempt, final Failed, frame and archive writes. All 36 mode/count
combinations must occur and every injected fault must be consumed. A dropped
Attempt causes exactly one extra handler call; dropped/hidden Failed writes
finish from the exhausted journaled budget without rerunning the handler.

The 100,000-seed run passed in 183.074 seconds with 300,000 choices and 29,688,450
transport events, maximum virtual time 16 seconds. First-ten exact replay,
cross-process seed-42 identity and the new pinned trace are checked. New workload
plus existing corpus passed under race in 41.690 seconds; the full simulator
suite passed in 120.702 seconds. A compiled production budget-reset overlay fails
the pin with an invalid step protocol in 0.008 seconds. Vet passed.
[Logs, mutation and hashes](scale/continuation-panic-model-2026-10-01/) bind the proof.
Runtime source remains unchanged from fbe427a.

This closes the seeded normal-budget/individual uncertain-write slice; it does
not simulate Raft or establish real replies behind an unconfirmed server delay.
Combined faults, actual Attempt-to-Failed SIGKILL and other continuation SDK
acceptance gates remain open, as do final-source full matrix/soak, online GC and
mixed seed 65's latency miss. The million-timer campaign remains live unchanged.

## Actual SIGKILL after final panic Attempt and before terminal state publication

A real Linux worker subprocess on an R3 three-node fixture consumes one panic
before each of two checkpoints. Its finish_v1 frame saves PanicAttempts=2 and
state/locals 20. A test-only operation observer holds its execution goroutine
at either the acknowledged third Attempt or acknowledged Failed append. The
parent independently confirms contiguous Attempt counts 1, 2, 3, the correct
journal tail, the verified frame and absent terminal state before sending and
verifying actual SIGKILL.

A replacement pinned to another peer recovers through the original unacknowledged
run; no synthetic wakeup is published. Every replacement handler is poisoned,
and its reader denies archives. Both cuts return exactly final poison with
no handler entries, one frame read and zero archive reads. The full pre-kill
journal prefix is unchanged. After Attempt, only Failed is appended under a
higher epoch; after Failed, the journal stays unchanged while terminal state is
materialized. All peers return the same failure and raw integrity finds one
invocation and one terminal.

The race test passed in 40.714 seconds with recovery samples 13.015 and 13.024
seconds and 19-entry final histories. A compiled production overlay bypassing
the exhausted-budget recovery check fails after_attempt in 19.826 seconds:
the replacement reruns its poisoned handler and changes the error. Vet passed.
[Logs, mutation and source hashes](scale/continuation-panic-kill-2026-10-01/)
retain the proof. Runtime source is unchanged from parent 3a5ed7e.

These are two actual process-death cuts and individual under-30-second samples,
not release p99 or full matrix/soak evidence. The existing Tier 1 panic workload
covers uncertain Attempt/Failed writes; a seeded process-kill schedule, combined
faults, server faults at these cuts and other continuation acceptance gates
remain open. Mixed seed 65 latency, online GC and remaining capacity campaigns
are unchanged. The million-timer runner continues live without restart.

## Reusable frame-held promise across replacement and child/parent retirement

A real R3 worker starts one child returning a spilled 614,402-byte JSON result.
The parent resolves its promise and checkpoints finish_v1; its 579-byte frame
retains outcome reference/hash and consumption facts without serializing the
derived result cache. A replacement pinned to another peer resumes after a gate,
awaits the saved promise twice and verifies detached return bytes. One real blob
read serves both awaits. No prefix handler or archive read occurs; the child
runs once and the logical history has one call and one child consumption.

The new optional worker.WithResultBlobPort wraps the existing real result
transport so loads are counted independently of journal/frame reads. All peers
return 614402. Full offline staged replay verifies the same result and all eight
SDK entries without another child invocation.

Child retirement is correctly refused until its parent is terminal. With all
worker loops stopped, later child retirement and quiescent sweep preserve its
exact result via the retained parent frame. After parent retirement, sweep
reclaims result/frame/archive (three objects). The raw audit after child
retirement finds one current parent and one terminal. A compiled collector
overlay omitting frame PromiseOutcomes reference marking deletes the result
prematurely and fails this exact assertion.

The real race contract passed in 18.250 seconds; mutation failed behaviorally
in 17.070 seconds. Worker race suite passed in 14.433 seconds, SDK race used its
unchanged cached pass, and vet passed. [Logs, mutation and source hashes](scale/continuation-promise-2026-10-01/)
retain the proof. This closes the focused real reusable-promise/retirement slice;
seeded integrated promises, SIGKILL/combined faults and remaining continuation
acceptance gates stay open, along with full matrix/soak, online GC and mixed
seed 65 latency. The million-timer runner remains live without restart.

## Seeded resolved promises, transient result reads and corruption controls

The production parent/child workers now run continuation_promise against seeded
transport. A real child execution produces a 614,402-byte spilled terminal
result; the parent consumes it, checkpoints its promise metadata and suspends
finish_v1. A fresh worker resumes with no archive reads and awaits the promise
twice, checking detached bytes and one successful verified blob read. The parent
initial handler remains at its two prefix entries, with one call_async and one
child consumption. Raw integrity requires both parent and child terminal.

Six modes cover normal operation, one/two unavailable resumed reads, dropped/
hidden-ACK child terminal-object writes and deliberate stored-object corruption.
Lost writes can repeat the child handler while retaining one child invocation
and notification; result-read retries never consume another signal. Corruption
must fail the parent with ErrCorruptJournal and is reported separately as an
expected rejection control.

The 100,000-schedule run passed in 735.908 seconds with 21,160,695 transport events
and a two-second virtual maximum: 83,194 successful recoveries and 16,806 expected
corruption rejections. First-ten exact replay, cross-process seed-42 identity,
all six modes and two final pins pass. New workload plus then-current corpus
passed under race in 75.563 seconds; final new pins passed separately in 1.220
seconds; full simulator suite passed in 137.897 seconds. Compiled mutations
omitting restored promise metadata and bypassing outcome hashing fail in 0.024
and 0.012 seconds. Vet passed. [Logs, patches and hashes](scale/continuation-promise-model-2026-10-01/)
retain the evidence. Runtime source is unchanged from parent 8f6789a.

Extended Tier 1 gets a 180-minute job / 170-minute Go timeout to accommodate the
added workloads after the earlier roughly 104-minute broad run. The normal
cluster suite gets 35/30 minutes after an earlier 18-minute suite and added
contracts; individual recovery bounds remain enforced. Combined faults, modeled
promise retirement/GC, actual promise SIGKILL/server cuts, remaining continuation
gates and independent full matrix/24-hour soak remain open. This includes
expected rejection controls and is not an unqualified all-success release gate.

## Retirement handler entries follow publication rather than fixed counts

[CI at 3a5ed7e](https://github.com/AntPAllen/js-wf/actions/runs/36805207254) returned
the correct reused result with four handler entries and three effects, failing
the retirement fixture's fixed three-entry assertion. The log does not establish
why the fourth entry happened. A controlled real manifest Create loss now
reproduces four entries/three effects: replay before runtime publication is
permitted and repairs the committed checkpoint without repeating its effect.

The fixture now directly requires that every fresh initial-handler entry sees
no published frame for its generation. It separately counts those entries,
retains exactly three effects and every stale-frame/result/collection check,
and requires its controlled loss to occur exactly once. Both baseline and loss
cases passed under race in 39.228 seconds. A compiled worker overlay ignoring
a published generation-3 frame fails that new guard in 18.561 seconds. Vet passed.
[Original CI log, new controls and hashes](scale/continuation-retirement-publication-2026-10-01/)
retain the proof. This corrects the fixture without changing runtime decisions
or claiming that the controlled fault explains the original CI interleaving.
Fresh CI validation remains pending. Mixed seed 65 latency and online GC stay
open; the million-timer runner remains live without restart.

## Continuation global entry budget and reserved terminal request

A worker-package real R3 contract follows the existing limit-fixture pattern:
both workers use a private 16-entry test budget while production retains its
100,000 default/hard cap. Two checkpoints leave a finish_v1 frame at logical
anchor 9 / absolute SDK position 8. The first worker suspends on a gate at
index 12, then stops. A replacement pinned to another peer uses no archive reads
and leaves prefix-stage entry counts unchanged.

The signal consumes index 13 and completes its wait at 14. The next effect
request cannot fit a completion plus terminal, so Failed occupies index 15
and retains the rejected declaration in LimitRequest. The effect never runs.
Full reconstruction has exactly 16 entries, terminal state matches the journal,
all peers return ErrTooLong and raw integrity finds one invocation/terminal.
An explicit CLI-style rejected-request substitution audits both checkpoints
and all 11 SDK entries offline, stopping at ErrReplayPendingStep with zero
effects; a changed request is nondeterministic.

The final race test passed in 17.983 seconds. A compiled production overlay using
suffix length instead of absolute indices for budget/reservation completes the
workflow incorrectly and fails the test in 16.850 seconds. Vet passed. [Logs,
patch and source hashes](scale/continuation-limit-2026-10-01/) retain the proof.
No production code changes are made. The full 100,000-entry continuation run,
seeded integrated limits, limit-adjacent fault cuts and remaining SDK gates stay
open, along with full matrix/soak, mixed seed 65 latency and online GC.

[CI at 8f6789a](https://github.com/AntPAllen/js-wf/actions/runs/36806383537) confirms
the earlier aggregate suite budget was exhausted at 20 minutes while its active
visibility test had run for six seconds. The full stack is retained in this
proof. The already-pushed 0821f8c suite-budget increase is awaiting fresh CI.
Its full 100,000-seed Tier 1 run and the million-timer campaign remain live
without restart.


## Active continuation canceled-timer wakeups and compatible version replay

A new real R3 contract exposed a runtime bug: a native wakeup for a canceled
timer was counted as a no-op but still entered an active continuation stage.
Its logical journal stayed unchanged. The real reproduction failed in 15.487
seconds; the production-worker seeded reproduction failed in 0.005 seconds.
The worker now returns before active handler dispatch once cancellation is
confirmed, while preserving generation/input/frame validation and terminal
outcome repair ahead of that return.

The real replacement-worker contract uses cancellation stored only in its frame
(absolute timer step 2 / frame SDK position 8), rejects archive reads and requires
no prefix/stage entries or journal changes on actual native delivery. After a
signal gate, Version=2 replays despite an increased supported maximum of 3.
Cross-peer result 2, raw integrity and full 12-step offline staged replay pass.
This and the existing terminal cancellation contract passed under race in
23.296 seconds. Worker race passed in 32.414 seconds; vet passed.

The new continuation_canceled_timer model passed 100,000 schedules in 140.654
seconds: seven fault modes times two deadlines, 200,000 choices and 23,960,419
transport events. All 14 combinations and every injected fault must occur;
first-ten exact replay, cross-process seed-42 identity and a new pinned trace
are checked. New workload plus full pinned corpus passed under race in 23.797
seconds. It also constructs terminal journal / absent outcome state and requires
canceled delivery to repair state without dispatch or journal changes; this is
a fixture boundary, not a claim that committed KV data disappears.

Compiled selected-pin controls fail behaviorally: original dispatch reenters
the stage in 0.009 seconds; placing the canceled return before terminal repair
leaves state absent in 0.013 seconds. [Logs, controls and source hashes](scale/continuation-canceled-timer-2026-10-01/)
retain the evidence. The fix applies to active ordinary invocations as well as
continuations; the complete simulator suite passed in 116.150 seconds.

[Standard CI at 0821f8c](https://github.com/AntPAllen/js-wf/actions/runs/36807803624)
and its mixed job passed, validating the preceding publication-fixture and
aggregate suite-budget changes. The 9b3831e mixed job also passed; its standard
suite and the full 100,000-seed-per-workload run at 0821f8c remain in progress.
Those jobs predate this runtime fix. Combined faults, remaining continuation
acceptance cuts, full final-source matrix/24-hour soak, online GC and mixed
seed 65 latency remain open. The original million-timer process remains live
without restart.


## Actual production continuation cap across checkpoints and replacement

The continuation limit fixture now also has an opt-in real R3 100,000-entry
case. Neither worker changes its default budget. Initial/middle stages each
add 24,996 SetState steps before their checkpoints. The first worker stops
with exactly 99,997 logical entries; finish_v1 is anchored at index 99,993 with
absolute SDK position 99,992. Its replacement rejects archive reads and leaves
prefix stage counts unchanged.

The gate occupies SignalConsumed 99,997 and StepCompleted 99,998. The next effect
cannot fit completion and terminal reservations, so Failed occupies 99,999 with
the rejected LimitRequest and zero effect executions. Full logical history is
exactly 100,000 entries, terminal state matches its payload, all peers return
ErrTooLong and raw integrity finds one invocation/terminal. Explicit CLI-style
rejected-request substitution replays both continuations and all 99,995 SDK
entries offline to ErrReplayPendingStep; changing the declaration is rejected.

The actual-cap run passed in 67.861 seconds; the shared small fixture passed
under race in 18.045 seconds. A compiled suffix-length-budget mutant completes
the small fixture incorrectly and fails its semantic assertion in 16.348
seconds. Vet passed. [Logs, patch and hashes](scale/continuation-production-limit-2026-10-01/)
retain the evidence. The manual journal-boundary-100000 workflow adds an
independent continuation job; hosted confirmation remains pending. Runtime
source is unchanged from 7e414b2.

This closes the full production-cap continuation slice previously left open.
Seeded integrated limits, limit-adjacent fault cuts and other continuation
acceptance gates remain open, along with final-source full matrix/24-hour soak,
mixed seed 65 latency and online GC. The earlier full simulation campaigns and
million-timer process remain live without restart. Standard CI at 9b3831e has
now passed, including its continuation limit contract.


## Actual SIGKILL before/after acknowledged checkpoint frame storage

Two earlier real R3 cuts now hold a child worker after its durable checkpoint
request and either before frame Put or after acknowledged frame Put, before
checkpoint completion. The parent confirms seven live journal records, a
pending finish_v1 declaration at index 6, absent manifest and prospective frame
identity/anchor/hash/SDK offset. Before Put the object is absent; after Put its
exact bytes are readable from another peer. The actual child exit is SIGKILL.
The incomplete checkpoint must not produce a suspended-scanner candidate.

A replacement pinned elsewhere recovers the original unacknowledged runs,
replays the pending declaration without another prefix effect and commits its
higher-epoch frame. The confirmed pre-kill prefix is byte-identical. Result 46,
state/locals, buffered signal, peer immutability, raw integrity and all 12 SDK
entries in offline staged replay pass. After acknowledged frame storage, the
abandoned old-epoch object remains immutable; quiescent sweep deletes exactly
that orphan and preserves the replacement frame and reconstructable history.

All six new/existing kill cases passed under race in 101.236 seconds. New kill
recovery samples are 13.136/13.132 seconds; these individual samples do not prove
release p99. A compiled replay mutant repeats the prefix callback and fails
before_frame's effect-count assertion in 16.267 seconds. Vet passed. [Logs,
patch and source hashes](scale/continuation-frame-kill-2026-10-01/) retain the
proof. Both hosted production-cap jobs at 02d5d19 also passed in
[run 36810716129](https://github.com/AntPAllen/js-wf/actions/runs/36810716129).
Runtime source is unchanged from 7e414b2. Remaining suspension/handoff cuts,
combined/server faults, modeled kill/GC and independent release gates stay open.

## Fresh mixed seed 12 latency miss at 7e414b2

[Mixed CI run 36810326702](https://github.com/AntPAllen/js-wf/actions/runs/36810326702)
passed seeds 1–11 then failed seed 12 terminal p99 at 40.574 seconds, before the
final integrity audit. Persistent 85 ms disk delay is on node 2, node 0 is
isolated/killed, and node 1 is paused. Two slow signal invocations spend measured
39.293/38.436 seconds across 48 replacement pre-append KV updates each, with zero
errors and only 60/76 microseconds total local gate wait. These measurements do
not establish the underlying server cause or causal connection to the recent
canceled-timer fix. [Downloaded logs, schedule, operation events, disk/Raft
attribution and grouped timings](scale/mixed-seed12-7e414b2-2026-10-01/) are retained.
No fencing or renewal guarantee has been relaxed. This miss and seed 65 remain
open, as do final-source full matrix/24-hour soak and online GC. The million-timer
process and both full simulation jobs remain live without restart.


## Actual SIGKILL after continuation suspension and handoff/release

Two additional real R3 child-worker cuts now stop after acknowledged Suspended
append and after acknowledged continuation enqueue plus lease release, before
original delivery ACK. Parent reads confirm nine logical records ending at
Suspended index 8, published frame, only two live journal messages and no live
signals. The consumer still has unacknowledged runs. After suspension there is
no expected handoff message and the lease remains present; after handoff/release
exactly one correctly addressed stable-ID run is retained and the lease is gone.

After verified SIGKILL, the scanner must enqueue one continuation candidate for
the suspension sequence. A pinned replacement denies archive reads and never
enters the prefix handler. Both effects run once; confirmed pre-kill prefix,
frame-buffered signal/state/locals, result 46, peer reads, raw integrity and all
12 offline SDK entries pass. Terminal scans enqueue nothing. Multiple durable
wakeups are available; recovery is not attributed to any single queued delivery.

The new cases passed under race in 21.693 seconds, with individual recovery
12.932 seconds and 127.391 ms. Compiled production controls skipping handoff
publication and suspended-continuation repair fail their selected assertions
in 3.280/3.199 seconds. Vet passed. [Logs, mutations and hashes](scale/continuation-handoff-kill-2026-10-01/)
retain evidence. No production runtime changes. Combined/server faults,
unknown handoff/release/ACK replies, modeled process cuts and full release gates
remain open; these two samples do not prove p99.

[Mixed CI at 25330a9](https://github.com/AntPAllen/js-wf/actions/runs/36811347097)
passed seed 1 then failed seed 2 terminal p99 at 47.748 seconds. The failed log
is retained with this proof; it predates these fixture-only changes and does
not establish a causal explanation. Seeds 2, 12 and 65 remain open. Full Tier 1
jobs and the million-timer campaign continue live without restart; online GC
and independent final-source matrix/24-hour soak remain open.


## Active continuation cancellation and durable notification recovery

A new seeded workload resumes a finish_v1 continuation from its frame, verifies
state 23/locals 45, enters one blocking effect, rejects a stale-generation
cancel and interrupts the active suffix on a matching cancel. The prefix stays
byte-identical; archive reads are forbidden, cancellation consumes once and
Failed has no effect completion. Seven modes cover cancel publish/enqueue
lost-before-commit and hidden acknowledgments, plus missed notification and
stale durable poll followed by a matching cancel. Production runtime source is
unchanged.

The workload passed 100,000 schedules in 74.582 seconds (16,988,200 events,
maximum virtual time 15 seconds), a 1,000-seed race run and exact first-ten/
separate-process seed-42 replay. The seed-42 regression is pinned and the full
pinned corpus passes under race. Its matching real R3 contract passed under
race in 49.719 seconds: notification cancellation took 116.826 ms, and removing
and flushing the core subscription made the production durable poll recover in
14.718 seconds. Both preserve ten prefix records, add exactly five suffix
records, run the effect once, avoid archive reads and return ErrCancelled
through all three peers with raw integrity passing. These are individual
samples, not p99.

Compiled production generation-key controls fail immediately on a stale cancel
in the seed helper and at the corresponding real assertion under race. Vet
passes. [Logs, mutation and hashes](scale/continuation-running-cancel-2026-10-01/)
retain the proof. Combined publication/GC/cancellation cuts, handlers ignoring
context, offline canceled-pending-effect replay and independent final release
gates remain open; this is not the complete continuation acceptance suite.


## Offline replay of canceled pending effects and early terminal identity

The active-cancellation seeded and real R3 histories now reconstruct offline
through all nine SDK entries. The pending effect request is verified without
executing its callback; ErrReplayPendingStep describes the offline stop while
the recorded Failed cancellation remains the workflow outcome. Both notification
and production durable-poll histories pass. A two-checkpoint unit covers 15 SDK
entries, changed pending declarations and invalid terminal payloads.

The focused proof found a runtime defect: Failed terminal invocation generation
was unchecked before replay handlers. The shared replay implementation now
validates terminal identity whenever supplied and requires an error for Failed.
Compiled old-runtime and callback-execution controls fail their exact semantic
assertions. Complete wf race, full default sim, seeded/pinned race, real R3 race,
100,000 expanded cancellation seeds and vet pass. Original trace bytes remain
unchanged. [Proof](scale/continuation-cancel-offline-2026-10-01/) retains results.
This closes the prior focused offline-cancellation gap; combined faults and
independent full release gates remain open.


## Seeded global journal budgets at continuation boundaries

The continuation_limit workload runs production workers with bounded modeled
budgets 16/18/20, two checkpoints and lost/hidden replies for gate consumption,
completion and reserved Failed publication. All 21 combinations preserve global
logical count, frame anchor and SDK offset; the next effect never runs, the
reserved failure slot remains available, and immutable outcome/raw integrity
and explicit CLI-style rejected-request replay pass. The modeled constructor
may only lower the budget; zero retains the 100,000 default. Production's
JetStream-facing constructor and hard journal cap are unchanged.

100,000 seeds, exact first-ten/separate-process seed-42 replay, full pinned
corpus under race, constructor bounds, real R3 budget-16 race and vet pass.
The compiled suffix-budget control fails its semantic count assertion in
0.006 seconds. [Proof](scale/continuation-limit-model-2026-10-01/) retains the
scope. The independent real 100,000-entry default-cap proof still stands;
small modeled budgets do not prove that scale anew. Combined real near-limit
reply/server/process faults, seeded retirement/GC and full release gates remain
open.

## Plugin registration and operator replay

`worker.WorkflowDefinition` now packages initial Handler and named Continuations
for Go plugins. `wf-worker` accepts a map/factory of definitions and registers
stages before execution; `wf replay` accepts a single definition/factory and
uses ReplayWithContinuations through specialized failure validators as well as
ordinary completion/suspension. Existing plugin forms remain supported.
[Real runner/R3 export tests, synthetic failure tails and compiled controls](scale/continuation-plugin-2026-10-01/)
prove basic registration/dispatch and offline effect suppression. Ordinary replay preserves typed missing-frame
and unknown-stage errors. Combined fault and full
release acceptance gates above remain open.

## Seeded retirement and quiescent GC

A continuation_retirement transport workload now exercises production SDK frames,
checkpoint manifest publication/purge, restoration, retirement and quiescent GC
on shared retained stores. Forty retirement/GC fault combinations preserve a
survivor's frame-held promise result; mark failures stop before deletion, and
GC between an uncertain retirement and its retry remains safe. Generation-bound
tombstones, raw terminal audits, exact replay and compiled missing-reference/
wrong-generation controls are verified. [100,000-seed proof and scope](scale/continuation-retirement-model-2026-10-01/)
retain evidence. This slice explicitly orchestrates SDK delivery cuts; worker
integration, reuse under combined faults and online GC remain open.


## Shared worker promises, retirement and combined publication/handoff cuts

Production parent/child dispatch and client Signal now run on shared retained
stores in the seeded worker_promise_retirement slice. Final 100,000 schedules
pass with lost notifications/object writes, replacement reads, uncertain retire-
ment/GC, complete-history offline replay and generation-correct tombstones.
The production 614,402-byte child spill is retained by a compact parent frame
through child retirement, then reclaimed with the parent frame/archive.

Eight actual worker SIGKILL publication/handoff cuts combined with all-three-
server SIGKILL/restart pass under race in 144.645 seconds, with unchanged confirmed
journal prefixes, higher successor epochs and sub-30-second raw kill/enabling
recovery. Published frames skip initial/archive reads; completed frames awaiting
manifest repair are reused. The two handoff cuts assert pre-kill lease and exact
run-message states. Compiled enqueue/release omissions are caught semantically.
[Seeded proof](scale/worker-promise-retirement-2026-10-01/) and
[combined cuts](scale/promise-handoff-full-restart-2026-10-01/) record source and
scope. These do not prove all reply-loss combinations, active-writer GC, TTL,
limit-adjacent combined faults or the independent full-matrix/24-hour gates.


## Bounded publication recovery

Archive/manifest publication, journal/signal purge, suspension append and handoff
share a fifteen-second deadline. Missing replies before or after commit recover
through existing NAK/release/redelivery; the stored frame and recorded prefix
remain unchanged. [Eight seeded/native cuts and compiled controls](scale/continuation-response-budget-2026-10-01/)
pass: final 100,000 seeds, exact replay, 143-pin corpus under race, and all eight
real R3 port-response cuts under race with 16.282–16.608s raw recovery. Frame
Put/Get during the handler and further combined limit/TTL/process cases remain
outside this proof, as do whole-matrix and soak release gates.


## Frame object request deadlines

Frame Put/Get through the worker result port now have separate fifteen-second
budgets while user effects keep their original context. [Four frame and five
spilled-result modes](scale/result-frame-response-budget-2026-10-01/) have seeded
exact/offline replay, 152-pin race and nine real R3 port-response proofs. Confirmed
frames are reused unchanged; a frame stored before completion can be orphaned
and replaced by a newly anchored frame. The first 100,000-seed campaign was
interrupted by the VM reboot; a fresh memory-bounded campaign is pending.
Further combined limits, lease/TTL and server/process faults remain open.

## Retirement reuse with manifest loss and actual state leader restart

A focused race control now combines quiescent retirement/GC, generation reuse,
rejection of an old collected frame and a controlled fresh-manifest response loss
with library shutdown/restart of the confirmed state-stream leader. It passes22.05s
with exactly one leader restart and manifest loss, two retired objects reclaimed,
three effects and two correct terminal generations; shared/survivor/fresh references
remain after collection. Original no-loss/loss controls pass43.40s through the same
helper. [Exact test overlay and local logs](scale/continuation-retirement-state-leader-2026-10-04/)
retain scope: ephemeral SDK/stores were not preserved. This is not OS SIGKILL,
concurrent-writer GC, lease/TTL/limit combinations or full-matrix/24h qualification.

## Retirement reuse with actual server SIGKILL

Both fresh-manifest response-loss cuts now pass under race with the confirmed
state leader killed/restarted and with all three servers killed before restart.
Observed SIGKILL wait statuses and distinct replacement PIDs are required.
Process test67.74s, library control25.74s; old-generation rejection, raw integrity,
exact effects and shared/survivor/fresh references remain strict. Actual SDK and
process stores/logs are preserved, with629 unchanged selected inputs and627 Git
matches plus exact test overlays. [Complete focused originals](scale/continuation-retirement-process-2026-10-04/corrected/)
verify; [first startup failures](scale/continuation-retirement-process-2026-10-04/first-startup-failed/)
remain separate. The new fixture uses bounded provisioning retries within its
original30s readiness budget. Worker SIGKILL, active-writer GC, lease/TTL/limit,
p99 and final-source matrix/24h combinations remain open; stores were not reopened.

## Retirement/reuse with manifest loss and lease expiry across server SIGKILL

A focused race cut now verifies the held owner/configured12s TTL, kills all three
servers, holds them down13.0003s and restarts. It passes57.78s with terminal epoch
78 above captured54, three effects, two terminals and preserved shared/survivor/
fresh references. Fresh initial repair executes three times without repeating its
recorded effect. Actual SDK/stores/logs and438 archive members verify;629 selected
inputs unchanged,628 Git matches plus exact test overlay. Original startup/
publication/scenario budgets and production TTL remain unchanged.
[Complete focused originals](scale/continuation-retirement-lease-expiry-2026-10-04/).
Stores were not reopened; worker SIGKILL, active-writer GC, other TTL/limit cuts,
p99 and final-source full-matrix/24h qualification remain open.


## Real JetStream domain retirement and generation reuse

Actual R3 WFRETIRE servers and domain-routed public clients/workers pass the
original strict retirement/reuse scenario under race23.44s at9581ebc. Generation
1→3, exactly two retired objects collected, old manifest rejected before user
code, one lost fresh-manifest reply repaired with three effects/two terminals.
All peers return fresh2; survivor1, exact shared bytes and fresh references remain.
Actual race SDK/650 Git inputs/stopped stores/1,076 archive members verify;
stores were not independently reopened. Original30s startup/60s scenario budgets
stay. [Complete domain proof](scale/continuation-retirement-domain-2026-10-05/).
Positive domain path accepted at recorded source; forced absence confirmation,
domain server faults/legacy versions, worker SIGKILL, active-writer GC and full
final-source matrices/24h remain open. No unchanged Tier1 graph rerun.


## Domain retirement/reuse with all-three library restart

Clean7a6531f racePASS31.78s; three old servers stopped before replacements,
three new IDs and WFRETIRE account API responses recover in5.810s under original
30s whole-cut target.60s scenario remains. Generation1→3, two reclaimed objects,
old-manifest rejection before user code, one lost fresh-manifest reply, three
effects/two terminals and all-peer/shared/survivor/fresh references verify.
Actual SDK/650 Git inputs/stopped stores/1,082 archive members verify; no store
reopen claim. [Complete proof and failures](scale/continuation-domain-all-server-restart-2026-10-05/).
Earlier added5s metadata gate failures stay preserved; reconnect alone did not
prove metadata leadership. Focused library restart accepted at executed source;
domain SIGKILL/forced absence confirmation/legacy versions, worker SIGKILL,
active-writer GC/full matrices/actual24h remain open. No unchanged Tier1 rerun.


## Retirement generation reuse across fresh-manifest worker SIGKILL

Actual clean4ab4bdd race35.39s, kill-to-result12.651s under original30s target.
Retired generation1/two collected objects, preserved survivor/shared content,
new generation3/old-manifest rejection, actual child fresh-manifest publication,
held epoch51 and reapedSIGKILL. Successor62 resumes one frame without archive
reads; exact three effects/two terminals/all-peer fresh2/survivor1/shared/fresh
references pass. Original30s startup/60s scenario/production12s lease stay.
Actual parent/child SDKs, three native server binaries/build fields,651 Git inputs,
stopped originals and1,084 archive members verify; stores not independently
reopened. [Complete worker-kill proof](scale/continuation-retirement-worker-sigkill-2026-10-05/).
Focused after-manifest retirement worker kill accepted at recorded source;
other retirement timing/server/domain/legacy combinations, active-writer GC,
full current-source matrices and actual24h remain. No unchanged Tier1 graph rerun.

### Worker crash before fresh manifest publication (2026-10-05)

The prepared frame is already referenced by the checkpoint StepCompleted in the
journal. A successor replays that record and republishes its manifest with the
recorded anchor, then resumes the continuation under a higher owner epoch.
Quiescent GC must retain this reachable frame. The initial retirement/reuse
fixture incorrectly required a replacement frame and orphan collection; its
failed native originals are preserved in
[the failure proof](scale/continuation-retirement-before-manifest-2026-10-05/failed-replacement-assumption/).
Corrected assertions retain the original recovery/effect/fencing targets; native
qualification passed at8da5935: race35.61s, recovery12.614s, epoch51→69,
exact effects/all-peer results/raw integrity and GC retention verified.
[Qualified originals](scale/continuation-retirement-before-manifest-2026-10-05/qualified/).
No bounded prefix-resume claim applies to this cut.

### Mixed encoding crash fixture — 2026-10-05

`TestContinuationRetirementProtobufWorkerSIGKILLToJSONSuccessor` prepares a
protobuf checkpoint anchor, reaps the owner with SIGKILL, then resumes with a
JSON writer. It verifies persisted wire formats, successor fencing, bounded
checkpoint reads, effect counts and quiescent GC retention. The opt-in fixture
uses the original30s recovery/12s lease/60s scenario gates. Native qualification
passed at07a419c: race35.43s, recovery12.747s, epoch51→62, actual stored
protobuf anchor independently decoded with generated Python codec and raw JSON
terminal decoded. One frame/zero archive/exact effects/all-peer/integrity/GC
checks pass. Full rolling/chaos remains open.

### Protobuf pre-manifest journal repair qualified — 2026-10-05

Atde673be the before-manifest counterpart passes under race34.62s, recovery12.846s,
epoch51→69. Missing manifest/prepared frame/durable protobuf completion verified
before reapedSIGKILL; JSON successor replays completion, republishes the same
reachable frame and finishes. Exact effects/all-peer/raw integrity/GC retention
pass. Independent Python codec binds actual protobuf content reference/hash/
index/epoch to the prospective manifest and repaired frame; JSON terminal decoded.
Read counters unobserved; no bounded-resume claim. Full rolling/chaos stays open.
[Complete originals](scale/continuation-retirement-protobuf-before-manifest-2026-10-05/).
