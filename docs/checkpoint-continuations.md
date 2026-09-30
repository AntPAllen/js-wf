# Materialized SDK checkpoints with named continuations

Status: continuation contract and SDK cursor foundation. Durable capture,
manifest publication, worker dispatch, suffix reads and recovery remain open.
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
versioned continuation names. A proposed `wf.Continue(ctx, nextStage, data)`
commits a checkpoint and ends the current delivery. Its caller must return the
continuation result immediately. The worker invokes the named next function
with the original invocation input and serialized continuation data. It creates
no second invocation and does not reset the journal, fencing epoch or limits.
These public APIs are proposed, not currently callable.

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
Durable frame storage and all remaining verification above still need implementation.
