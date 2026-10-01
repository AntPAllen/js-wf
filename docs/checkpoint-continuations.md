# Materialized SDK checkpoints with named continuations

Status: continuation contract, SDK cursor foundation, bounded frame codec and
SDK capture/restore, archive/runtime publication and prefix-free journal reads.
SDK Continue frame/pair publication is implemented. Worker dispatch and recovery
remain open.
This document preserves the plan's checkpoint requirement; it does not count
as a completed checkpoint feature.

## Execution contract

An ordinary Go handler starts at its entry point on replay. Loading its final
state at that point cannot recreate its earlier stack, local variables or
control flow. It also changes earlier `GetState` observations. Checkpointed
workflows therefore opt into registered, named continuation functions. Each
boundary serializes the local data needed by the next function. Existing
ordinary handlers retain their full replay contract.

The proposed registration contains an initial handler and a map of stable,
versioned continuation names. `wf.Continue(ctx, nextStage, data)`
commits a checkpoint and ends the current delivery. Its caller must return the
continuation result immediately. The worker invokes the named next function
with the original invocation input and serialized continuation data. It creates
no second invocation and does not reset the journal, fencing epoch or limits.
Continue is callable on a context with continuation support configured. Worker
registration/dispatch APIs remain proposed; ordinary workers return
ErrContinuationUnsupported.

Changing a stage's implementation still requires ordinary replay/version
compatibility within that stage. Incompatible boundaries get a new name, and
old names remain registered for retained checkpoints and full offline replay.
An unknown stage or unsupported checkpoint format fails before executing effects.

## Frame and identity

A versioned, size-bounded checkpoint frame contains:

- Workflow type, ID and invocation stream sequence.
- Versioned next-stage name and JSON continuation data.
- Materialized SDK state values, consumed signal identities and resolved promise
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
No worker currently supplies a checkpoint offset.

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
Durable frame storage and the remaining integrated verification above still need implementation.

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
live-handle rules. No production worker calls them yet. The worker must check
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

These are low-level SDK primitives. `wf.Continue`, stage registration and durable
worker publication/dispatch remain proposed, and ordinary workers still replay
from the initial handler. Frame hash/anchor binding is not a substitute for
checking the actual retained checkpoint completion and contiguous journal suffix.

## Implemented archive/runtime publication

`journal.WriteCheckpointSnapshot` verifies an already stored frame and committed
checkpoint pair, archives the prefix strictly before its completion anchor, and
publishes an archive/runtime manifest with revision CAS. The frame hash is bound
to the completion and its serialized data hash to the checkpoint request. SDK
positions are counted independently of auxiliary journal entries. Purge remains
a separate confirmed step and preserves the live completion anchor. Newer runtime
pointers reject older replacement attempts; generic compaction cannot remove
the anchor. `wf.Continue` and frame/pair publication are still not wired.

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
read while SDK state and 150 recorded effects replay correctly. There are still
no production worker callers. Stage dispatch must reject unknown registrations
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
contract. Worker code still does not configure SetContinuationSupport or dispatch
stages. The registry, suffix append counters/runtime facts, handoff repair,
offline stage replay and worker process-kill proof remain open.
