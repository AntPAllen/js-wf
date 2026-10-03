# JetStream Durable Workflow Runtime — Implementation Plan

Sep 27, 2026 · @Anthony Allen

Repository copy of the supplied plan. The Tier 1 simulation section under
Distributed verification was expanded on Sep 28, 2026. Cross-stream batch
claims were corrected after implementation showed that JetStream atomic
publishing is scoped to one stream.

## Scope and architecture

Build a Restate-style durable execution runtime on NATS JetStream 2.12+, in Go, with a Go SDK first. Restate-style means journal-and-suspend: user code runs forward, every awaited step's result is written to a per-invocation journal, and a resumed invocation replays journal entries instead of re-running side effects. This is chosen over Temporal-style full history replay because it needs far less SDK machinery and maps one-to-one onto JetStream's per-subject CAS append.

&#91;embedded content: runtime architecture · 4 JetStream stores, 1 new runtime\]

The only new stateful component is the SDK runtime inside the worker; every durable primitive is a stream or a KV bucket, and the dispatcher is stateless.

**Storage layout**

| Store | Subjects | Config that matters | Role |
| --- | --- | --- | --- |
| `WF_INV` stream | `wf.inv.<type>.<id>` | `MaxMsgsPerSubject=1`, `DiscardNewPerSubject`, limits retention | Start-once idempotency record; holds input + start metadata |
| `WF_RUN` stream | `wf.run.<partition>` (mapped from `wf.run.<type>.<id>` via `{{partition(N, 2, 3)}}`) | WorkQueue retention, default consumer `AckWait` 13 s (12 s lease TTL plus 1 s), `MaxDeliver` unlimited with backoff | Dispatch queue; one durable pull consumer per partition |
| `WF_JRN` stream | `wf.jrn.<type>.<id>` | Limits retention, `DenyPurge=false`, `Nats-Expected-Last-Subject-Sequence` on every publish | Per-invocation journal; entries `{epoch, index, kind, payload}` |
| `WF_SIG` stream | `wf.sig.<type>.<id>.<name>` | Limits retention, `Nats-Msg-Id` dedup window 2 min | External signals; merged into the journal by the worker |
| `WF_LEASE` KV | key `<type>.<id>` | Per-key TTL 12 s, `LimitMarkerTTL` | Single-writer lease; value = `{worker, epoch}` |
| `WF_STATE` KV | key `<type>.<id>` | History 1, revision CAS | Snapshot + terminal result; bounds journal replay |

[NATS atomic batch publishing](https://docs.nats.io/nats-concepts/jetstream/streams) commits messages within one stream. It cannot atomically join `WF_INV` to `WF_RUN`, or `WF_SIG` to `WF_RUN`, because those streams need different retention policies. Ordered writes, stable message IDs, and durable repair scanners are required on every supported server version.

**Invariants every phase must keep** (these are the properties the distributed tests assert, numbered so later sections can cite them):

1. **I1 Start-once.** For any `(type, id)`, at most one `WF_INV` record and at most one journal ever exist, however many concurrent starts arrive.
2. **I2 Single writer.** Journal entries for one invocation are totally ordered by `(epoch, index)`; no two workers ever append with the same epoch, and a lower epoch never appends after a higher one has.
3. **I3 Effect-once outcome.** A side effect may physically run more than once (at-least-once), but exactly one result is recorded per journal index, and user code only ever observes that result.
4. **I4 Deterministic resume.** Replaying journal entries `0..k` through the user function produces the same sequence of step requests as the original run, or the runtime detects the divergence and halts that invocation with a non-determinism error.
5. **I5 No lost wakeups.** Every timer, signal, or child completion eventually produces a `WF_RUN` message for its target (liveness under crash and partition).
6. **I6 Durable completion.** Once any client observes a terminal result, every later read returns the same result for the retention window.

## Phase 0 — Test harness and cluster fixtures

Nothing else starts until a test can kill a JetStream node mid-publish and assert an invariant afterwards. Building this first is the difference between "works on my laptop" and a distributed system.

**Deliverables**

- [ ] `testcluster` package: boots a 3-node JetStream cluster in-process (`nats-server` as a library, `server.Options` with JetStream + cluster routes), returns clients pinned to each node. Boot under 3 s.
- [ ] Fault injector with four verbs: `KillNode(i)`, `PartitionNodes(a, b)` (via a TCP proxy such as toxiproxy or an in-process route filter), `PauseNode(i)` (SIGSTOP or a blocking hook, to simulate GC / long pauses), `SlowDisk(i, latency)`.
- [ ] Deterministic fault scheduler: seeds from `FAULT_SEED`, records the fault schedule to a file, replays it from a file. Every failing test prints its seed.
- [ ] Invariant checker library: given the raw contents of `WF_INV`, `WF_JRN`, `WF_STATE` after a test, mechanically checks I1, I2, I3 and I6 from the stream data alone (no test-local bookkeeping). I4 and I5 get checkers in phases 4 and 5.
- [ ] Client-side operation history recorder: every SDK call (`start`, `signal`, `getResult`) logs `{invoke_ts, return_ts, op, args, result}` in the format a linearizability checker (Porcupine) consumes.
- [ ] CI job matrix: `unit` (single-node, no faults), `cluster` (3-node, no faults), `chaos` (3-node, random faults, 20 seeds), `soak` (nightly, 2 h, 200 seeds).

**Proof of completion**

- Test: publish 10 000 messages to a plain stream while `KillNode(leader)` fires at a random point. Assert: all acked publishes present, no gaps in sequence, and the invariant checker's stream-integrity pass is green. Establishes the harness can detect a lost ack.
- Test: the same run with a pinned seed produces byte-identical fault schedules on two machines.
- Negative control: deliberately break replication (`Replicas=1`) and confirm the same test now fails. A harness that cannot fail is not a harness.

**Edge cases to test here**

- Leader election of the stream's Raft group happens during a publish: the publisher gets a timeout, not an ack. The test must treat "no ack" as "unknown", never as "failed".
- Consumer leader moves during a `Fetch`; in-flight messages get redelivered. Record and count redeliveries so later phases can assert on them.
- Clock skew between nodes (inject ±5 s via a wrapped clock in the SDK, not the OS) so nothing in later phases depends on wall time agreement.

## Phase 1 — Idempotent invocation (I1)

`start(type, id, input)` becomes a single publish to `wf.inv.<type>.<id>` on a stream with `MaxMsgsPerSubject=1` and `DiscardNewPerSubject`. Every publish also carries `Nats-Expected-Last-Subject-Sequence: 0`, so a retained invocation cannot be replaced by a later write even if the per-subject limit is ineffective during cluster movement. The server rejects the second and later publishes; the SDK reads the retained invocation, compares its input hash, and returns either `ErrAlreadyStarted` with its original handle or `ErrInputMismatch`.

The start must also enqueue the first run. Publish `WF_INV` first, then `WF_RUN` with `Nats-Msg-Id = start:<type>:<id>`. The dispatcher-side reconciler scans retained `WF_INV` records with no journal and repeats the enqueue after a crash or uncertain acknowledgment. This two-write repair path is required on both 2.12+ and older servers because the records belong to different streams.

**Deliverables**

- [ ] `client.Start` with `ErrAlreadyStarted` semantics and a returned handle `{type, id, invSeq}`.
- [ ] Stream provisioning code that is idempotent (`CreateOrUpdateStream`) and asserts the config it finds matches what it wants; a mismatch is a startup error, not a silent adopt.
- [ ] Durable two-write Start path with stable message IDs, unknown-outcome handling, and a repair scanner on every supported server version.
- [ ] Input size guard: inputs above `max_payload` (default 1 MiB) go to an Object Store bucket with only the object key in `WF_INV`.

**Proof of completion**

- Test: 500 goroutines across 3 clients pinned to 3 different nodes call `Start` with the same id simultaneously. Assert exactly one `WF_INV` message, exactly one `WF_RUN` message, 499 `ErrAlreadyStarted`, 0 other errors. Run under `chaos` with `KillNode(leader)` during the burst.
- Test: 100 000 distinct ids, then count subjects in `WF_INV` equals 100 000 and `WF_RUN` message count equals 100 000. Repeat with `PartitionNodes` toggling every 200 ms.
- Linearizability: Porcupine model where `Start` is a register write-once; the recorded history must be linearizable under all chaos seeds.

**Edge cases**

- Publish times out with no ack. The client must retry with the same payload; the second attempt returns `ErrAlreadyStarted` if the first actually landed. Test by injecting a proxy that drops the ack but not the publish.
- A client retries a start with a *different* input for the same id. Decision: reject with `ErrInputMismatch` by comparing an input hash stored in the `WF_INV` headers. Test both matching and mismatching retries.
- The same id reused after the previous invocation completed and was purged. Decision: allowed only after purge; test that a purge followed by a start creates a fresh journal with epoch 0 and that nothing from the old journal is visible (this is why `WF_JRN` purge must complete before `WF_INV` purge).
- Subject cardinality: `DiscardNewPerSubject` requires the per-subject index; measure memory for 10 M subjects on a 3-node cluster and record it in the doc's risk section.
- `Nats-Msg-Id` dedup window elapsing between a failed and a retried publish (window default 2 min). The fallback path must tolerate a duplicate `WF_RUN` message, which phase 3's lease makes harmless; test by setting the window to 1 s.
- Server version below 2.12 on one node only (rolling upgrade): the two-write Start and repair path must preserve I1 and I5 through leader movement. Native timer mode must fail closed if cluster-wide support is unproven. Test with a mixed-version cluster fixture.
- Simulate a one-shot failure of the `WF_INV` per-subject limit after a prior invocation has committed. A production `Start` retry must still preserve the first sequence through its subject-tail CAS; a control publish without the CAS header must demonstrate that the model would accept a second generation. Check the retained sequence before and after an actual mixed-version rolling upgrade.

## Phase 2 — Journal with CAS append (I2, I6)

The journal is the system. Every entry is published to `wf.jrn.<type>.<id>` with `Nats-Expected-Last-Subject-Sequence` set to the stream sequence of the entry the writer last saw. A stale writer gets a `wrong last sequence` error and must stop. Entries carry `{epoch, index, kind, payload, worker_id}`; `kind` is one of `Started`, `StepRequested`, `StepCompleted`, `Suspended`, `SignalConsumed`, `Completed`, `Failed`.

The journal reader is an ordered consumer filtered to the subject, started from the snapshot's sequence if `WF_STATE` has one. Snapshot every N entries (start with 256): write `{last_seq, epoch, state_blob}` to `WF_STATE` with revision CAS, then entries before `last_seq` may be dropped by per-subject purge with `Keep` semantics.

**Deliverables**

- [ ] `journal.Append(entry, expectedSeq) (seq, error)` distinguishing `ErrStale` (CAS lost), `ErrUnknown` (timeout), and success.
- [ ] `journal.Read(type, id) (entries, tailSeq)` that resumes from a snapshot and verifies `index` is contiguous.
- [ ] Snapshot writer with CAS on the `WF_STATE` revision and a journal purge that keeps the last K entries after the snapshot.
- [ ] Journal integrity checker (extends the phase 0 checker): for every subject, `(epoch, index)` strictly increasing, exactly one `Started`, at most one terminal entry, no entry after a terminal.

**Proof of completion**

- Test: two writers holding the same `expectedSeq` race to append 10 000 times; exactly one wins each round, the loser always sees `ErrStale`, never a silent success. Run with the stream leader killed every 500 appends.
- Test: append 5 000 entries with snapshots every 256 and purges after each; `Read` returns the same logical sequence as an un-snapshotted control run.
- Test: on `ErrUnknown`, the writer re-reads the tail and finds either its entry (retry succeeded) or not (retry needed), never a foreign entry at its intended index. This is the ack-lost case and it must be exercised 1 000 times under `chaos`.
- Throughput baseline recorded: appends per second per invocation and across 1 000 concurrent invocations on the 3-node fixture (`Replicas=3`, file storage). Regressions of more than 20% fail CI. `cas-throughput` builds a pinned reference and candidate on the same isolated runner, alternates three rounds, and requires both median rates to meet the 80% bar. Preserve every report and reject incompatible runtimes, malformed measurements and failed benchmark processes. The reference changes through an explicit source edit. The first hosted-runner gate passed at `7446a8f`; retain this check for future relevant changes.

**Edge cases**

- **Lost ack on append.** Covered above; the recovery rule is "re-read tail, compare `(epoch, index, worker_id)`".
- **Snapshot written, purge failed.** Reader must handle entries older than the snapshot still being present and skip them by sequence, not by index.
- **Purge succeeded, snapshot write failed** (wrong order). Forbidden by construction: purge only after the snapshot CAS acks. Test the crash between the two by killing the worker after the CAS and confirming the next reader still has a complete view.
- **Entry larger than `max_payload`.** Step results above \~900 KiB go to Object Store; the entry holds the key. Test with a 5 MiB step result.
- **Stream leader changes between `Read` and `Append`.** The `expectedSeq` remains valid because sequences are stream-global; test that a leader change alone never causes `ErrStale`.
- **Duplicate index from the same epoch** (a bug in the writer): the integrity checker must catch it, and the reader must refuse to resume rather than pick one.
- **Subject with millions of entries** (a runaway loop that never suspends). Enforce a max journal length per invocation (say 100 000); exceeding it fails the invocation with `ErrJournalTooLong`. Test the boundary.
- **Retention limits eviction** (`MaxBytes` on the stream hit): eviction of a live journal is data loss. Alert on stream usage at 70%; test that `DiscardNew` at the stream level turns appends into errors rather than evicting old journals (`Discard=new`, not `old`, on `WF_JRN`).

## Phase 3 — Dispatch and single-writer (I2)

This is where most durable-execution systems have their worst bugs, so it gets two independent mechanisms that must both agree before a worker touches a journal: a partitioned consumer that makes concurrent delivery rare, and a lease with a fencing epoch that makes it harmless when it happens anyway.

Dispatch: `WF_RUN` messages are published to `wf.run.<type>.<id>` and a stream subject transform maps them to `wf.run.<partition>` with `{{partition(N, 2, 3)}}` hashing on type and id, so all runs for one invocation land in one partition. N durable pull consumers, one per partition, each with `MaxAckPending` tuned for throughput. Workers own partitions through a simple assignment in a KV bucket (start static, N=64; rebalancing is a later phase).

Lease: before opening a journal the worker does `WF_LEASE.Create(key, {worker, epoch: last_epoch+1})` (fails if present) or `Update(key, ..., revision)` on an expired one. The epoch it wins becomes the epoch on every journal entry, and phase 2's CAS append rejects any lower epoch by construction: a fenced writer's `expectedSeq` is stale the moment the new epoch's `Started` entry lands. Lease TTL 12 s, renewed every 3 s; a renewal failure stops the worker before its next append. The shorter TTL leaves takeover time inside the under-30-second fault-latency gate after a hard worker kill; provisioning rejects an existing bucket with a different TTL so the old recovery contract cannot silently persist.

The five-container worker-kill smoke uses this same production TTL. Its historical 31.1-second result with a 30-second lease is a configuration mismatch, not a runtime defect to reproduce. Keep the current twelve-second TTL and thirty-second gate; do not spend additional runs on the old configuration mismatch.

For an existing 20 s or 30 s `WF_LEASE` bucket, stop workers and lease-holding loops before upgrading. Update that bucket's TTL to 12 s through JetStream KV management while preserving its other configuration and retained keys, verify the reported TTL, then start the new version. `provision.Ensure` refuses to run against the old TTL and never rewrites the bucket implicitly. Do not shorten TTL while old workers still hold live leases.

**Deliverables**

- [ ] Subject transform + N partition consumers, provisioned idempotently.
- [ ] `lease.Acquire(type, id) (epoch, error)`, `lease.Renew`, `lease.Release`; all with KV revision CAS.
- [ ] Worker run loop: fetch → acquire lease → read journal → run until suspend or terminal → write `Suspended`/terminal entry → release lease → ack `WF_RUN` message. On any lease or CAS failure: stop, do not ack (let redelivery retry), log the fencing event.
- A fetched run that finds a healthy owner naks with a five-second delay. It remains recoverable after owner failure without repeatedly racing the active writer when many signal or timer wakeups arrive for one invocation.
- [ ] In-progress heartbeat: `msg.InProgress()` every `AckWait/3` while running so long steps don't trigger redelivery.
- [ ] Metrics: fencing events, lease acquisitions per invocation, redeliveries, time-from-enqueue-to-lease.

**Proof of completion**

- Test (the one that matters): 200 invocations, each a loop of 50 steps that increment a counter in the journal. 6 workers. Every 2 s one of: kill a worker, pause a worker for 45 s (longer than the lease), partition a worker from the cluster. After 5 min: every invocation completes with counter exactly 50, and the integrity checker shows I2 holds on every journal. Any journal with two epochs interleaved fails the run.
- Test: paused worker resumes after lease expiry and tries to append. Assert it gets `ErrStale`, never a success. Assert a metric `fencing_events_total` incremented and the message was not acked by the fenced worker.
- Test: `MaxAckPending=1000`, 10 000 short invocations across 64 partitions, all complete; no invocation is ever executed by two workers simultaneously (checked by a test-only "currently running" set in a KV with CAS; any collision fails).
- Test: kill the `WF_RUN` consumer leader repeatedly; every message is eventually delivered and acked, with a recorded redelivery count.

**Edge cases**

- **Zombie worker after lease expiry.** Covered above; the CAS append is the last line of defence and the test must prove the lease alone was not what saved it (disable the lease in a debug build and confirm the CAS still rejects).
- **Lease renewal succeeds but the worker is then paused for longer than the TTL** before its next append. The append must carry an `expectedSeq` from before the pause, so it fails; assert this explicitly.
- **Two `WF_RUN` messages for the same invocation in flight at once** (timer fired while a signal arrived). The second acquirer finds the lease held, must nak with delay (not ack, not spin) and back off. Test that the invocation still processes both wakeups and that the loser does not starve.
- **Worker acks the `WF_RUN` message, then crashes before releasing the lease.** The lease TTL handles it; assert no wakeup is lost because the journal's `Suspended` entry records what it is waiting on and the reconciler (phase 7) re-enqueues.
- **Worker crashes after appending `Completed` but before ack.** Redelivery finds a terminal journal and acks immediately without running; test that no `Started` for a new epoch is written.

Terminal duplicate processing also has a [seeded read-cost and real 500-child
consumer-fault proof](scale/terminal-owned-wakeups-2026-10-01/). Bounded local
hints select a durable outcome/current-generation probe after lease acquisition;
uncertain probes fall back to journal replay, parent notification remains
required, and automatic snapshot repair must finish before the shortcut is
enabled. The 100ms virtual full-read cost isolates repeated terminal replay;
it does not attribute the observed delay to a NATS server mechanism. This
focused proof does not replace the full fault matrix or final-source gates.

Automatic ordinary-handler snapshot work also has a fifteen-second attempt
budget. The [missing-response proof](scale/worker-snapshot-budget-2026-10-01/)
checks the production context in Tier 1 and verifies actual R3 heartbeat fencing,
release, suspended signal recovery and unchanged recorded effects while a
snapshot-port reply is withheld. Snapshot publication/purge uncertainty still
requires repair before enabling terminal hints. This proves bounded recovery
under that response contract; the original real journal-row stalls have no
stack evidence identifying snapshot work as their cause.
- **Partition rebalance while a message is in flight.** Two workers may hold the same partition's consumer for a moment; the lease makes this safe. Test by reassigning partitions every 5 s during the chaos run.
- **Poison invocation** (user code panics every time). `MaxDeliver` unlimited with exponential backoff capped at 5 min, plus a per-invocation attempt counter in the journal; after a configurable count, write `Failed` and stop. Test the count is honoured across worker restarts.
- **Hot partition** (one tenant floods one partition). N=64 static partitions cannot fix this; record it as a known limit and test that other partitions keep their latency.
- **`AckWait` shorter than a legitimate long step** without heartbeats: assert redelivery happens and the second worker is fenced, then assert that with heartbeats it does not happen.

Automatic membership coordinator takeover must invalidate every retained assignment
revision before balancing, including assignments whose owner remains unchanged.
Renewing a coordinator lease in a separate bucket cannot fence an assignment
already paused between that renewal and its CAS. A new coordinator claims all
64 keys with same-owner CAS writes (initializing missing/deleted keys), renewing
its lease before each write. Any read, write, or CAS uncertainty stops the pass;
only a fully acknowledged claim permits balancing. Subsequent passes by the
same coordinator do not repeat the claim. Workers retain loops on same-owner
revision changes. Prove the unchanged-owner pause boundary in Tier 1 and on a
real three-node cluster, plus pauses during the claim itself, partial-claim failure and tombstone recovery.
This fences older revisions after the claim completes; it is not an atomic
transaction across the membership and assignment buckets.

## Phase 4 — SDK core: journal-and-suspend (I3, I4)

User code is an ordinary Go function `func(ctx wf.Context, input T) (R, error)`. Every durable operation goes through `ctx`: `ctx.Run(name, fn)` for a side effect, `ctx.Sleep(d)`, `ctx.Await(promise)`, `ctx.Signal(name)`. The runtime keeps a cursor into the journal. On each call it checks: is there a `StepCompleted` at this index? If yes, return its result without running anything. If there is a `StepRequested` but no completion, re-run the effect (at-least-once) and CAS-append the completion. If neither, append `StepRequested`, run, append `StepCompleted`.

Determinism guard: `StepRequested` carries the step `name` and a hash of the step's declared inputs. On replay, a mismatch between the journal's entry and what the code asks for at that index halts the invocation with `ErrNonDeterministic` naming the index, the recorded name and the requested name. This catches the common failure (code changed under a running invocation) without needing Temporal-style full sandboxing.

Suspension: when an await cannot complete, the runtime appends `Suspended{waiting_on}`, releases the lease and acks the run message. Nothing holds a goroutine or a connection while waiting. Resumption replays from the snapshot and continues past the `Suspended` entry once the awaited entry exists.

**Deliverables**

- [ ] `wf.Context` with `Run`, `Sleep`, `Await`, `Signal`, `Call` (child invocation), `SetState`/`GetState` (backed by the snapshot, journaled as entries).
- [ ] Two-entry step protocol (`StepRequested` then `StepCompleted`) with the recovery table implemented and unit-tested for all four journal states at a given index.
- [ ] Determinism guard with a clear error carrying index, expected and actual.
- [ ] Code versioning hook: `ctx.Version(changeID, min, max)` journaled as an entry so a deploy can branch on it, the same shape as Temporal's `GetVersion`.
- [ ] Replay harness: `wf.Replay(journalBytes, fn)` that runs a function against a recorded journal offline with no NATS. This is also the unit-test tool for user workflows.
- [ ] Materialized SDK checkpoints with explicit named continuations: persist locals/runtime state, resume a registered stage without archived-prefix reads, preserve absolute identities and fencing/limits, and prove publication, repair, offline replay and retirement cuts. The [continuation contract](checkpoint-continuations.md) records implementation and remaining acceptance gates.

**Proof of completion**

- Test: a workflow with 20 steps, each step increments an external counter (a plain in-memory map behind a mutex). Kill the worker at every one of the 41 possible points (before/after each of the 2 appends per step, plus the start). After all runs, the workflow result is correct in 41 of 41 cases and the external counter shows at most 2 increments per step (at-least-once effect, exactly-once outcome, I3).
- Test: record a journal, change the workflow code so step 7 has a different name, replay; assert `ErrNonDeterministic{index: 7}`. Change step 7's *input* only; same assertion. Change step 21 (after the recorded tail); assert no error (I4).
- Test: the phase 3 chaos run repeated with real SDK workflows instead of the counter loop, same pass criteria.
- Property test: generate random workflow DAGs (steps, sleeps of 0, awaits on immediately-resolved promises), run once cleanly to produce a journal, then run under random kill points and assert the final result equals the clean result. 10 000 cases in CI.

**Edge cases**

- **Step returns a non-serialisable value** (channels, funcs, cyclic structs). Fail at `StepRequested` time with a typed error, before the effect runs; test with each.
- **Effect runs, then the `StepCompleted` append gets `ErrUnknown`.** Re-read tail: if the completion is there, continue; if not, the recovery rule re-runs the effect. Document loudly that effects must be idempotent or tolerate a repeat; provide `ctx.RunOnce(idempotencyKey, fn)` that passes the journal index as the key so downstream systems can dedup.
- **User code reads wall time, random numbers or goroutine-scheduled state.** Provide `ctx.Now()` and `ctx.Random()` that journal their values; a linter that flags `time.Now` and `rand.` inside workflow functions. Test the linter on a corpus.
- **Goroutines spawned inside workflow code.** Not supported in v1; the determinism guard catches reordering. Test that two steps issued from two goroutines produce `ErrNonDeterministic` on replay at least sometimes, which proves the guard rather than the feature.
- **Very long-running invocation crossing a code deploy.** `ctx.Version` test: old journal + new code with the version branch replays clean; old journal + new code without the branch fails clean.
- **Panic inside a step.** Caught, recorded as `StepCompleted{error}`, surfaced to user code as an error; retry policy is user code's choice via `ctx.Run` options. Test panic, `runtime.Goexit`, and a step that blocks forever (must respect `ctx` cancellation from lease loss).
- **Step result identical across a retry but with a different epoch.** Both writers cannot both succeed (I2); assert the surviving completion's epoch equals the lease epoch at the time.
- **Journal cursor and snapshot disagree** (snapshot says index 300, journal tail says 298 because a purge kept too little). Refuse to run with `ErrJournalGap`; test by hand-corrupting a fixture.

## Phase 5 — Timers and sleeps (I5)

`ctx.Sleep(d)` appends `StepRequested{kind: timer, fire_at}` then publishes a `WF_RUN` message for the invocation with a `Nats-Schedule` header for `fire_at` (2.12 message scheduling) and `Nats-Msg-Id = timer:<type>:<id>:<index>`, then suspends. When it fires, the worker resumes, sees the timer entry and that `now >= fire_at`, appends `StepCompleted` and continues. If it resumes early for another reason (a signal), the timer stays pending and the scheduled message still fires later.

Admission also matters: a fresh deployment uses the retained `WF_TIMER` fallback by default because one connected server version cannot certify every peer. Native scheduling requires an explicit operator choice after all peer versions have been checked. Automatic provisioning rejects an existing native stream until the operator selects native mode explicitly.

Order matters: journal entry first, scheduled publish second. A crash between them leaves a journal saying "waiting on timer" with no scheduled message, which is exactly what the reconciler in phase 7 looks for. The reverse order would produce a wakeup for an invocation that has no record of wanting one, which is harmless but wasteful, so the order is a correctness choice only in combination with the reconciler.

**Deliverables**

- [ ] `ctx.Sleep`, `ctx.Timer(name, d)` returning an awaitable, and cancellation via `timer.Cancel()` which journals a `StepCompleted{cancelled}` and lets the eventual scheduled wakeup no-op.
- [ ] Scheduled publish with a fallback for pre-2.12 servers: a `WF_TIMER` stream with per-message TTL and a poller that re-enqueues on expiry (slower, but the same contract).
- [ ] Timer coalescing: a resumed invocation completes every timer whose `fire_at` has passed in one run, not one run per timer.
- [ ] Timer metrics: scheduled, fired, late-by histogram, cancelled-but-fired no-ops.

**Proof of completion**

- Test: 10 000 invocations each sleep a random 1–60 s. All complete; measure lateness. Pass: p99 lateness under 2 s with no faults. For the route-fault recovery test, measure the 30 s p99 gate from the later of each timer's `fire_at` and the final route heal; report raw `fire_at`-to-completion p99 separately. Zero invocations stuck after 5 min past their latest `fire_at` (I5).
- Test: kill the worker between the journal append and the scheduled publish, 200 times. Without the reconciler these invocations stall (assert that, it validates the test); with the reconciler enabled all complete.
- Test: a 30-day sleep on a cluster that is fully restarted (all 3 nodes) twice during the test with the clock advanced by a wrapped clock injected into the server. The scheduled message survives restarts and fires.
- Test: cancel a timer 100 ms before it fires, 1 000 times. The invocation never observes the timer as fired; the late wakeup message is acked as a no-op and counted in metrics.

**Edge cases**

- **Timer fires while the invocation is running** (another wakeup holds the lease). The second `WF_RUN` message naks with delay; the running worker's timer coalescing may already have completed the timer, in which case the redelivered message is a no-op. Test both interleavings.
- **Sleep of zero or negative duration.** Complete immediately in the same run; no message published. Test.
- **Sleep longer than the stream's `MaxAge` on `WF_RUN`.** Scheduled messages must not be aged out; set `MaxAge=0` on `WF_RUN` and test a sleep longer than any other stream's `MaxAge`.
- **Clock skew between the server that stores the schedule and the worker's `now`.** Timer completion uses the journal's `fire_at` versus the *server* timestamp on the wakeup message, never the worker's clock. Test with ±5 s worker skew from phase 0.
- **Thousands of timers due in the same second** (a cron-like fan-in). Measure delivery rate; document the ceiling; ensure the consumer's `MaxAckPending` does not become the bottleneck.
- **Timer for an invocation that has since completed** (completed via a signal path). Wakeup finds a terminal journal, acks, no-op. Test.
- **Timer for an invocation that has been purged.** Wakeup finds no `WF_INV` record; ack and log at debug. Must not create a journal. Test.
- **Duplicate scheduled publish** (client retried after `ErrUnknown`). `Nats-Msg-Id` dedups within the window; outside the window two wakeups arrive and the second is a no-op. Test with the window set to 1 s.

Fresh positive timer requests must not consume the current delivery's older
wakeup timestamp, even if a leader clock change makes that timestamp appear
after the new deadline. Sleep and timer Await/selection now enforce this rule;
recorded due timers retain coalescing on replay. The sustained behind-clock
row exposed and preserves a confirmed early-completion regression in
[the clock failure proof](scale/tier3-mixed-server-clock-2026-10-01/behind-ten-minute-early-timer-failure/).
This fix does not certify all clock-source transition cases.

## Phase 6 — Signals, promises and inter-workflow calls

External writers must never append to a journal directly, because that would race the worker's CAS. Instead a signal is a publish to `wf.sig.<type>.<id>.<name>` followed by a `WF_RUN` wakeup with a stable message ID. A signal scanner repairs the wakeup after a crash or uncertain acknowledgment between those writes. The worker, holding the lease, reads pending signals from `WF_SIG` with an ordered consumer from the last consumed sequence (recorded in the journal as `SignalConsumed{sig_seq}`) and journals them in order. Signals are therefore delivered in `WF_SIG` sequence order, exactly once into the journal, and a signal that arrives before the invocation asks for it is buffered by the stream itself.

A child call `ctx.Call(childType, childID, input)` is: journal `StepRequested{call}`, then `Start` the child with `childID` derived deterministically from parent id + index (so a retry hits `ErrAlreadyStarted`), then suspend. Worker child creation/read/enqueue has one five-second whole-operation budget in addition to individual request deadlines. An uncertain start leaves the request unfinished for replay under the same deterministic child identity and parent generation. The child's terminal step publishes a signal `result` to the parent. A durable promise is a signal with a name the user chose and a `ctx.Await` on it; external systems resolve it through the client API.

**Deliverables**

- [ ] `client.Signal(type, id, name, payload)` with `Nats-Msg-Id` for client idempotency; `ctx.Signal(name)` returning a channel-like awaitable.
- [ ] Signal drain step in the worker loop, with `SignalConsumed` journal entries and a per-subject purge of `WF_SIG` up to the consumed sequence at snapshot time.
- [ ] `ctx.Call` with deterministic child ids and result-as-signal; `ctx.CallAsync` returning a promise.
- [ ] Client `Await(type, id)` for results: watch `WF_STATE` key or read the terminal journal entry; returns the same bytes forever (I6).

**Proof of completion**

- Test: 100 signallers send 100 signals each (10 000 total, numbered) to one invocation while chaos runs. The invocation records them in `WF_SIG` sequence order with no gaps and no duplicates in the journal; every signaller's history is linearizable as a queue append.
- Test: signal sent before `Start` (race at creation). The invocation sees it on its first drain. Test the reverse race: `Start` then signal 1 ms later.
- Test: fan-out of 1 parent → 500 children → all results back. Kill the parent worker at random points. Exactly 500 child journals exist (I1 on children via deterministic ids), the parent's result is the sum of all children, no child ran twice as a new invocation. Include child-creation and result-collection cuts, including after the first and last result; preserve the committed parent prefix and require a higher successor epoch. The result-collection fixture is `TestFiveHundredChildFanoutAfterParentResultSIGKILL`. The opt-in `TestFiveHundredChildFanoutParentBoundaryMatrix` covers first, seeded interior and last positions in both creation and result collection, with complete pre-kill prefix preservation and higher successor epochs. Run with `WF_FANOUT_BOUNDARY_MATRIX=1`; its CI guard requires all six cases to execute and pass.
- Test: a 3-deep parent→child→grandchild chain where the middle worker dies after the grandchild completes but before the parent is signalled. The reconciler or the grandchild's `Nats-Msg-Id`-protected signal retry gets the result to the parent.

**Edge cases**

- **Signal to a completed invocation.** Stream accepts it; the wakeup finds a terminal journal and acks. The client gets no error unless it asked for `RequireRunning`. Test both.
- **Signal to an invocation that does not exist.** Decision: signal-with-start (Temporal's semantics) is opt-in; without it, the client gets `ErrNotFound` from a `WF_INV` lookup. Test the lookup race with a concurrent start.
- **Signal payload above `max_payload`.** Object Store spill, same as journal entries.
- **Signal drained into the journal, then the worker dies before processing it.** The journal has `SignalConsumed`; replay delivers it to user code again from the journal, never from `WF_SIG`. Test.
- **Signal drained but `SignalConsumed` append lost** (`ErrUnknown`). Re-read tail; the drain cursor comes from the journal, so the worst case is re-reading the same `WF_SIG` messages and appending them under the same index, which the CAS makes safe. Test 1 000 times under chaos.
- **`WF_SIG` purge races a late drain** on a fenced worker: the fenced worker cannot append anyway (I2). Test.
- **Child id collision** between two different parents (deterministic id scheme bug). Include the parent id in the hash and test that two parents with the same step index produce distinct child ids.
- **Parent cancelled while children run.** v1: children are not cancelled automatically; document it. Add `ctx.Call` option `CancelWithParent` in v2 with a cancellation signal.
- **Client `Await` on a result that was purged.** Return `ErrPurged`, not `ErrNotFound`, using a tombstone in `WF_STATE` that outlives the journal by the configured grace period. Test the ordering of purges.

## Phase 7 — Reconciler, retention and visibility

The reconciler is the liveness backstop for I5. It is a single-leader loop (leader elected with a `WF_LEASE` key) that scans for invocations whose journal says `Suspended{waiting_on}` but for which no corresponding wakeup can be found or whose expected wakeup time has passed by more than a grace period, and re-enqueues a `WF_RUN` message with `Nats-Msg-Id = reconcile:<type>:<id>:<jrn_seq>`. Because every wakeup is idempotent (phases 3 and 5), the reconciler can be aggressive and wrong without causing harm; it only needs to never be too quiet.

Retention is a purge pipeline, strictly ordered: `WF_SIG` subject, then `WF_JRN` subject, then `WF_STATE` value replaced by a tombstone with TTL, then `WF_INV` subject. Reversing any pair of these opens a window where a fresh `Start` with a reused id can see old data.

Visibility is a projection: a durable consumer over `WF_JRN` (all subjects) writes `{type, id, status, started, updated, waiting_on, custom search attributes}` into a queryable store. Start with a KV bucket per index (status → set of ids) for small deployments and a Postgres or ClickHouse sink behind the same interface for large ones. The projection is rebuildable from the journal at any time, so it is allowed to be wrong briefly.

**Deliverables**

- [ ] Reconciler with leader election, scan cadence, per-scan budget, and a dry-run mode that reports what it would re-enqueue.
- [ ] Purge pipeline as a workflow in the runtime itself (dogfooding; the ordering constraint is exactly what a durable workflow is for).
- [ ] Visibility projection with a `Rebuild()` that replays `WF_JRN` from sequence 1 and a `Lag()` metric.
- [ ] Operator CLI: `wf list`, `wf describe <id>` (pretty-prints the journal), `wf replay <id>` (offline, from phase 4), `wf cancel`, `wf purge`.

**Proof of completion**

- Test: every stall-inducing kill point from phases 5 and 6 (append-then-crash-before-publish) re-run with the reconciler on. 100% of invocations complete within grace period + one scan interval. With the reconciler off, the same test reports the expected stalls (control).
- Test: reconciler leader killed every 10 s during the run above; still 100% completion; no invocation gets more than `ceil(scan_count)` duplicate wakeups.
- Test: purge 10 000 completed invocations while 10 000 others are running and 1 000 new starts reuse purged ids. Zero cross-contamination: every reused-id journal begins with `Started{epoch: 0, index: 0}` and no entry references a prior epoch.
- Test: kill the projection consumer, run 50 000 invocations, restart it; `Lag()` drains to zero and a full `Rebuild()` produces byte-identical projection state.

**Edge cases**

- **Reconciler re-enqueues an invocation that is actually running.** The lease holder wins; the extra message naks and later no-ops. Test it happens and is harmless.
- **Reconciler falls behind** (scan takes longer than the interval). Budgeted scans with a cursor in KV; test with 1 M suspended invocations on the fixture and confirm the cursor advances monotonically across leader changes.
- **Two reconciler leaders** (lease expiry during a pause). Both may re-enqueue; `Nats-Msg-Id` dedups within the window and idempotent wakeups cover the rest. Test with a 45 s pause.
- **Purge of a journal that a fenced worker is still reading.** The reader gets a gap or an empty subject and must exit without writing; assert no new `WF_STATE` write from it.
- **Purge pipeline crashes midway.** It is itself a workflow, so it resumes; test every step boundary.
- **Stream `MaxMsgsPerSubject` on `WF_JRN` misconfigured** (someone sets it to bound journal size). That silently drops old entries; the provisioning assertion from phase 1 must reject the config. Test.
- **Projection reads a journal for an invocation already purged.** Emits a delete; a later rebuild must produce the same absence. Test.
- **Search attribute changes shape** (a user renames a field). Projection versioning: store the schema version with each row and support a rebuild with a mapper.

## Distributed verification

Per-phase tests prove each mechanism; this layer proves the whole thing under adversarial conditions, in three tiers that cost progressively more and run progressively less often.

**Tier 1 — Deterministic simulation (every commit).** Run the production client, journal, lease, worker, and reconciler state machines against a seeded in-memory transport. A scheduler owns virtual time and chooses every transport response, wakeup, redelivery, and actor step. A seed plus a recorded decision trace must replay the same state transitions and checker result byte-for-byte. This tier explores runtime logic rapidly; it does not simulate Raft, disk persistence, or undocumented NATS behavior. The real-cluster tiers remain independent evidence.

**Implementation contract and boundaries**

1. Introduce a narrow internal transport port for only the operations production code uses: publish and publish-with-headers, stream info/read/purge, durable fetch/ack/nak/progress, KV create/get/update/delete/watch, Object Store put/get/delete, and server-time/scheduling calls. Keep public NATS-facing constructors. A production adapter calls nats.go; the simulator implements the same port. Move one vertical slice at a time (journal CAS and lease, then start/dispatch, then signals/timers/children/retention) so tests run production decisions, not a rewritten test-only runtime. Do not implement the entire JetStream client interface as the model.
2. Use cooperative actors, not uncontrolled Go goroutine timing, for simulated clients, workers, reconcilers, and servers. Every transport call, timer, lease renewal, handler effect boundary, and durable consumer action is a yield point. Actors share a virtual monotonic clock; wall-clock offsets are explicit per actor. Choose the next enabled action from a seed, record its actor and action ID, and fail replay if the enabled set or chosen action differs. Apply a maximum-step bound and report deadlock or livelock with pending actions.
3. Model the used JetStream subset: global and per-subject stream sequences with retained sequence holes; expected-last-subject CAS; MaxMsgsPerSubject/discard behavior; publish deduplication windows; stream and consumer retention; explicit ack, nak, progress, AckWait, redelivery, and consumer restart; KV revisions, create/update/delete and lease expiry; Object Store references; scheduled wakeups. Faults must distinguish a request lost before commit, a commit whose acknowledgment is lost, a stale read, a transient error, a delayed response, and a redelivery. A successful acknowledgment is never rolled back in the model. If server semantics are uncertain, wait for a real NATS contract test before adding them.
4. Store traces as versioned JSON: seed, model/adapter version, workload, initial state, scheduler choices, injected faults, transport requests and responses, virtual timestamps, and final checker result. FAULT_SEED selects generation; FAULT_TRACE replays a file without random choices. Failures print both and retain the trace as a CI artifact. Minimize a failure by removing decisions or actors while preserving the same invariant failure. A pinned trace must replay identically on different machines and with the race detector.
5. Run the existing raw-state invariant checker for I1, I2, I3, and I6 after every terminal schedule and at selected intermediate cuts. Add I4 checks by replaying recorded step requests, and I5 checks against virtual time: after faults heal, every enabled invocation either advances or yields a bounded, named reason it cannot. Feed client operations into the existing Porcupine history models. Track duplicate effects, stale epochs, retries, and reconciler actions even when the final state is valid.
6. Differentially check the model's transport contract, not only final workflow outputs. For each modeled API edge, run a small real three-node NATS fixture test and compare allowed responses and retained-state outcomes, including lost acknowledgments, CAS races, lease expiry, and redelivery. Replay every Tier 2/3 failure in Tier 1 when representable; otherwise add a contract fixture and extend the model. A failure only in simulation is a model or runtime candidate; a failure only on real NATS is a model-gap, environment, or server candidate. Do not label it a NATS bug from that difference alone.

**Build order and proof gates**

- sim/scheduler: deterministic virtual clock, cooperative actors, decision trace, exact replay, and shrinker. Prove two separate processes produce byte-identical traces from the same seed, and that changing a recorded enabled action makes replay fail closed.
- sim/jetstream: in-memory stream/KV/consumer/Object Store subset with explicit fault hooks. Contract tests compare each modeled edge with the real fixture. A lost-ack case must commit once, return an unknown result, and permit a safe retry; a dropped-before-commit case must leave no record.
- Runtime adapter: execute the same journal/lease/start/worker/reconciler code through production and simulated transports. Prove CAS single-writer fencing, start-once, effect-outcome once, deterministic resume, wakeup repair, terminal immutability, purge/reuse isolation, and snapshot/rebuild under injected cuts. Include a negative control for each of I1–I6; every checker must fail for its deliberate mutation.
- Focused production mutation CI: use Go overlays to remove the journal CAS option, make invocation leases private to each worker, reverse invocation retirement ordering, disable step replay comparison, remove run message IDs, and skip start scans. Each selected fixture must pass unmodified and then fail with specific semantic evidence. Build errors, unrelated failures, skips and timeouts do not count. The runner must itself reject deliberate build and unrelated-failure controls. Preserve logs, source hashes and modeled traces. This focused gate supplements the full mixed-workload six-mutation chaos release requirement; it does not replace it. See [the mutation runner](invariant-mutations.md).
- Sustained production mutation CI: `invariant-mutations-sustained` runs all six categories independently. Each intact/mutated pair executes the original ten-minute mixed journal-leader row and all its release checks, then admits its controlled live counterexample on the same retained stores with an additional verified SIGKILL. The original cohort must remain valid afterward. Require all six actual ten-minute semantic detections at the recorded source; `35s` is smoke only. This campaign does not replace the full 200-seed matrix or 24-hour soak. See [sustained mutation evidence requirements](invariant-mutations.md#sustained-mixed-chaos-and-retained-store-challenges).
- CI: first gate a fixed regression corpus plus 1,000 seeded schedules per commit; raise to 10,000 per commit once measured on CI hardware within a few minutes. Record seeds, trace paths, model version, steps/s, and virtual-time coverage. Keep 100,000 clean seeds and all known regression traces as the release gate. A timeout, unexplained skipped action, or model/real contract mismatch fails the gate rather than counting as a clean seed.

Start with the journal CAS/lost-ack vertical slice because it has a real three-node fixture and an unresolved server-side observation in the status record. The first useful result is a trace showing whether the runtime mishandles an unchanged-tail rejection under the modeled server contract; any real-only discrepancy then has a small API-level fixture to investigate. Follow with consumer-leader movement and timer route faults, where real runs have likewise exposed unexplained latency.

The WorkQueue transport model distinguishes consumer ack commitment from
physical stream removal. Seeded controls reproduce acknowledged retained
records with no pending deliveries or redelivery, and the stream-level drain
checker must reject that state. Explicit modeled removal completions do not
establish a real-server repair protocol; expected failing liveness controls
must be reported separately from clean runtime schedules.

**Tier 2 — Real cluster chaos (every merge).** Phase 0's 3-node fixture with the fault injector. The workload is a mixed generator: 40% short workflows, 30% timer-heavy, 20% signal-heavy, 10% deep fan-out. Faults follow a schedule from the chaos matrix below, 20 seeds per run, 10 min each.

The sustained journal- and consumer-leader rows are available as `TestMixedMatrixJournalLeaderEveryThirtySeconds` and `TestMixedMatrixConsumerLeaderEveryThirtySeconds` with `WF_MATRIX_CHAOS=1`. Its default duration is ten minutes on one three-process cluster, with the selected leader killed every 30 seconds and verified replica catch-up after each restart. The consumer row selects an active durable where possible and records its name, pending count, and ack-pending count before the kill. Repeated seeded batches preserve the 40/30/20/10 parent mix; fan-outs include children and grandchildren. It checks client histories, intermediate and final retained state, per-workload terminal and next-entry p99, every start/timer/signal/child enabling event, completion within five minutes of the final heal, and a drained run queue. `tier2-matrix-journal.yml` exposes the `tier2-matrix-leaders` workflow with a `journal` or `consumer` row selector. It runs 1, 20, or 200 consecutive seeds as independent jobs and retains histories, dispatch events, actual fault times, and per-invocation liveness samples. `WF_MATRIX_DURATION=35s` is a smoke run and is explicitly excluded from ten-minute evidence. A third row, `TestMixedMatrixAllServersKilledEveryThirtySeconds`, SIGKILLs all three servers before restarting any, preserves their ports and file stores, and confirms full replica catch-up for all ten provisioned workflow streams, buckets, and object stores before declaring heal. Select `cluster` in the same workflow. The p99 gate remains measured from enabling events for this process-kill row. A fourth row, `TestMixedMatrixServerPartitionEveryThirtySeconds`, isolates node 2 for ten seconds while clients and workers stay on the two-node majority. It confirms 4/4/0 route counts and a replicated majority write during isolation; after healing it checks 8/8/8 routes, the retained probe through node 2, and full workflow-store replica catch-up. Select `partition` in the workflow. Its raw enabling-event p99 remains under 30 seconds because the majority retains quorum. A fifth row, `TestMixedMatrixRandomWorkerKilledEveryFiveSeconds`, runs three separate worker processes and SIGKILLs a seeded random worker every five seconds, verifies its exit signal, then starts a new process generation. It uses the same parent mix, with two-second short effects to expose in-flight kills, and requires at least one kill of a worker with an active lease. It records PID, worker generation, active-lease count, each child dispatch log, and the shared audits; select `worker` in the workflow. Earlier twenty- and ten-second AckWait configurations missed the gate under successive hard kills. A thirteen-second AckWait has passed a shortened active-kill run; sustained release validation remains open. These five rows do not replace the remaining rows or the full-matrix release gate.

A sixth sustained row, `TestMixedMatrixWorkerPausedFortyFiveSeconds`, runs the mixed workload on three worker processes and pauses an active worker for 45 seconds using SIGSTOP. The seeded candidate order prefers an active worker; `/proc` state confirms stop and resume. Faults begin at +5s and repeat every minute, keeping each pause past the twelve-second lease TTL while peers continue. After SIGCONT, an active pause must report lost-lease or stale-append evidence from the resumed process. The shared history, integrity, latency and queue-drain gates remain required. Select `pause` in the matrix workflow. A 35s workload smoke still completes its full 45s pause and is excluded from sustained evidence.

A seventh sustained row, `TestMixedMatrixWorkerReplyIsolationFortyFiveSeconds`, holds server-to-worker replies for 45 seconds while continuing to forward worker requests. Each worker is pinned to its own client TCP proxy and ignores discovered peers. Seeded selection uses an acquisition handoff: a child holds a newly acquired delivery until the parent installs the reply hold, preventing a released lease from being selected through a delayed active-count marker. The fault artifact names that delivery, and fencing must belong to it. Relay byte counters confirm the asymmetric fault, and a fresh successful worker PING confirms recovery after replies resume. An active isolation must report fencing. Faults begin at +5s and repeat each minute. Shared histories, retained integrity, raw enabling-event latency and queue-drain gates remain required; select `isolation` in the workflow. This is a worker transport fault with the cluster quorum intact.

The shared matrix subprocess helper retains typed fencing JSONL with actual PID,
process sequence and invocation/delivery/epoch identity. Completed fencing records
are synced before callbacks return. Graceful exit cross-checks the production
counter and retains final metrics. SIGKILL may interrupt the last record and has
no final metrics snapshot; consumers must preserve this uncertainty. Focused
lease-revocation proof does not certify a sustained R5 process-fault row.

The sustained worker-clock row is available as `TestMixedMatrixWorkerClockSkew`
and `workerclock` in `tier2-matrix-leaders`. Three separate worker processes
run with verified Go wall-clock offsets of +5 seconds, −5 seconds and zero
throughout the ten-minute mixed workload. The parent verifies their clocks
against retained JetStream probe timestamps initially and every thirty seconds;
these are clock observations, not process kills. It retains the shared
history, cohort/final integrity, per-type terminal/next-entry p99, final-enabling
completion and queue-drain gates. The CI selector supports 1, 20 or 200 seeds;
35-second runs remain smoke-only. Server skew and the other missing sustained
rows still require their own fixtures and release-count validation.



The sustained server-clock rows are `TestMixedMatrixServerClockSkewPositive`
and `TestMixedMatrixServerClockSkewNegative`, exposed as `serverclockplus` and
`serverclockminus` in the matrix workflow. One actual NATS process runs with
its Go wall clock shifted by +60 or −60 seconds; peers and workers retain normal
clocks. Startup, every thirty seconds and final checks verify all three clocks
through `/varz` and require the invocation, run, journal, signal, state and lease
stream leaders to remain on the skewed node. Latency artifacts record the
known server offset and convert retained server timestamps and timer deadlines
to the parent clock domain. Raw stored records are unchanged. The shared
history, integrity, per-type p99, five-minute completion and queue-drain gates
remain required. This is sustained static skew coverage; it does not combine
skew with leader movement or other faults.

The sustained restart-mid-fan-out row is
`TestMixedMatrixFanoutRestartEveryThirtySeconds` (`fanoutrestart` in CI).
At each thirty-second boundary it holds newly entered grandchild effects,
requires a suspended parent with six distinct durable child requests and at
least one unfinished child, then SIGKILLs all three servers before restarting
any on their retained stores. Full workflow-store replica catch-up precedes
release of the effect barrier. The retained parent journal prefix must be
unchanged; final checks require exactly the same six completed children and
two completed grandchildren per child. It preserves the shared mixed workload,
history, integrity, raw enabling-event p99 and queue-drain gates. The barrier
is fault orchestration; the handler bodies and child protocol execute in the
production worker. Smoke runs do not establish sustained release coverage.

The sustained five-second block-device stall row is
`TestMixedMatrixBlockDiskStallEveryThirtySeconds` (`blockdisk` in CI).
Node 2's entire JetStream store resides on a private ext4 filesystem backed by
a sparse image, loop device and device-mapper linear target. Before each stall,
workflow stream leaders are moved to that node; suspending its mapped device
blocks all filesystem block requests, including files created after startup.
Each fault proves a dirty-file sync stayed blocked for the full five seconds,
then resumes the device and verifies full workflow-store replica catch-up.
The shared mixed workload and raw enabling-event latency gates apply. This
requires passwordless sudo, loop devices and device mapper. It supplies real
block-device stall coverage; Tier 3's separate dm-delay per-request injection
and five-node full-matrix soak remain required.

The sustained rolling-upgrade row is `TestMixedMatrixRollingServerUpgrade`
(`upgrade` in CI). It starts three actual NATS 2.11.17 processes with a verified
fallback-timer deployment and runs the shared ten-minute mixed workload while
upgrading each server once to the module-pinned version on its retained store.
The seed chooses node order. In the ten-minute row, upgrades begin at +30 seconds,
+5 minutes and +9 minutes 30 seconds, preserving long mixed-version intervals
and a final fully upgraded workload interval. Every transition records all
peer versions before/after, requires replica catch-up for all eleven stores
including `WF_TIMER`, and verifies fallback provisioning remains unchanged.
The production fallback scanner runs throughout; the ordinary raw enabling-event
p99, histories, integrity and queue-drain gates apply. A 35-second smoke upgrades
only one node and cannot prove the full three-node transition.

The sustained matrix workflow also accepts `row=all`, expanding all thirteen
fault variants at the same source revision. At 200 seeds this requires 2,600
row/seed executions; groups of up to twelve consecutive seeds produce 221
hosted jobs within the matrix limit. Every job retains per-seed fault/state
artifacts and Go JSON results. The result guard rejects skipped/missing tests,
failed or duplicate results and shortened duration. Groups stop on a failed
seed; omitted subsequent seeds do not count clean. A one-seed all-row campaign
checks coverage at that revision, while only all 200 successful seeds per row
can clear Tier 2. See [campaign runner scope](scale/full-matrix-campaign-2026-10-01/).

The production `wf-worker -events-file FILE` option records actual fencing and
start, signal, suspended, timer and fallback-timer publication decisions in
JSONL, with process identity and session sequence. Graceful shutdown drains and
syncs the bounded asynchronous writer; queue overflow and I/O failures are fatal.
Each process needs its own file. SIGKILL can lose queued or unsynced records;
complete hard-kill causal review still requires independently retained evidence.
The subprocess repair proof does not clear the full mixed process-fault gate.

**Tier 3 — Jepsen-style (nightly and before release).** Five real VMs or containers with real network partitions (iptables), separately verified server and worker clock skew, disk stalls (dm-delay), and process kills including `SIGKILL` of the NATS server with unsynced writes (`sync_interval` set to the production value). Client histories recorded as in phase 0 and checked with Porcupine against these models: `Start` as write-once register; `Signal` as an ordered queue per invocation; `Await` as a read of a register that becomes immutable at first non-empty read. Plus the stream-level invariant checker over the final state.

Implementation proceeds through individually verified sustained five-container rows before the full 24-hour matrix. Optional repair observers cover start, signal, suspended waits, due journal timers and fallback timers. Timer records retain the fire time and journal request sequence; fallback records retain timer-stream sequence, invocation generation and step. Each event records the actual publication acknowledgement, uncertainty or dry-run decision before any fallback timer deletion, so a deletion failure cannot erase an acknowledged wakeup. The journal-leader row uses stable published endpoints, production R5 stores and sync interval, mixed workloads, thirty-second SIGKILL/restart cadence, retained histories and final physical consumer drain. A 35-second smoke or ten-minute single-row result never clears the full Tier 3 release gate. The dedicated result guard rejects skipped tests, incomplete fault counts, missing workload cells and any full-matrix release claim. The consumer-leader row shares those gates, chooses a confirmed WF_RUN durable leader with a preference for pending deliveries, records every selection and its activity, and requires at least one active selection. Its selected consumer must recover current R5 replicas after each retained-store restart; idle-only selections cannot certify that row. The all-server SIGKILL row kills and confirms removal of all five servers before restarting any retained store, records ordered operation timestamps and requires restored R5 readiness. Its artifact guard rejects rolling restarts, partial kills and reversed timelines. This row retains the raw enabling-event p99 gate; the route-specific heal-time exception does not apply. The separate mid-fan-out row holds real grandchild effects while selecting a suspended six-child parent with unfinished children, then performs the same all-down restart. It retains cut-time parent/child journals, the exact recovered prefix and final parent/child/grandchild journals; the gate verifies the original six children and two completed grandchildren per child. The effect barrier is a test fixture, not a production delay. An additional sustained R5 route-quorum row isolates three servers, confirms zero routes on each and an unacknowledged R5 probe publication, then confirms all five route meshes and workflow replica recovery before setting heal time. It reports raw p99 alongside the recovery p99 from the later enabling event or last overlapping confirmed heal. Samples wholly outside an outage retain their ordinary delay, so healthy-period stalls remain visible. This quorum-removing slice does not replace the matrix row that requires progress on the majority side of a one-server partition. The separate majority route row cuts one server other than the shared client’s connected server, verifies a publication acknowledgement and advancement of WF_JRN during confirmed isolation, then checks full route/R5 recovery. It retains the raw enabling-event latency gate and does not apply the quorum-loss adjustment. Both route rows explicitly use a one-second fixture route ping; default-ping coverage remains separate.


Clock-skew injection must fail closed unless the running process reports the requested offset before the workload starts. The pinned Go server and a cgo-enabled Go probe bypassed `libfaketime`'s `LD_PRELOAD` clock interposition even though the same preload shifted `date`; [libfaketime documents this runtime limitation](https://github.com/wolfcw/libfaketime). The five-container server-clock slice instead builds the pinned NATS source with a test-only Go `time.Now` wall-clock overlay and verifies each node's own `/varz` time. The worker-clock slice uses the same overlay to build a separate worker process and checks its `time.Now` against an unshifted server's `/varz` before starting work. Both slices now cover one timer, one ordered-signal workflow, 100 short journaled effects, and a six-child fan-out with child-result signal and journal checks. The server slice keeps `WF_RUN`, `WF_SIG`, `WF_JRN`, and `KV_WF_STATE` leaders on the skewed node. These focused checks do not replace sustained mixed workloads and faults across the full matrix.

**Chaos matrix** (each row is a fault; each column is a workload the fault runs against; every cell must be green):

| Fault | Short | Timer-heavy | Signal-heavy | Fan-out |
| --- | --- | --- | --- | --- |
| Kill stream leader (`WF_JRN`) every 30 s | I1, I2, I6 | I5 | I2, order | I1 on children |
| Kill consumer leader (`WF_RUN`) every 30 s | completion | I5 | completion | completion |
| Kill random worker every 5 s | I2, I3 | I5 | I2 | I2 |
| Pause worker 45 s (past lease) | fencing count > 0, I2 | I2 | I2 | I2 |
| Partition one server from two | progress on majority side | I5 | I2 | I1 |
| Partition worker from cluster (asymmetric) | fencing, I2 | I5 | I2 | I2 |
| Clock skew ±5 s on workers | I3 | timer lateness bound | order | I3 |
| Clock skew ±60 s on one server | I6 | I5, lateness | order | I6 |
| Disk stall 5 s on one server | latency only, all invariants | I5 | I2 | I2 |
| `SIGKILL` all servers, restart | I1, I2, I6, no gaps | I5 | order | I1 |
| Full cluster restart mid-fan-out | I6 | I5 | order | exactly N children |
| Rolling server upgrade | two-write Start and repair preserve I1/I5 | I5; native timers fail closed until cluster support is proven | order | I1 |

**Liveness, not just safety.** Every tier records for each invocation the wall time from its last enabling event (start, timer due, signal sent, child completed) to its next journal entry. A safety-correct system that stalls is a failure: the pass bar is p99 under 30 s during faults and 100% completion within 5 min of the last fault healing. For a route fault that deliberately removes quorum, measure the p99 recovery gate from the later of the enabling event and the final confirmed route heal. Also report the raw delay from the enabling event so the outage remains visible. An invocation that completes before healing contributes zero post-heal delay.

The original 30-second consumer `AckWait` left no room for processing before the 30-second p99 recovery gate when a nak was lost during quorum loss. The runtime default is now thirteen seconds (the twelve-second lease TTL plus one second) with a three-second progress heartbeat; explicit 30-second control fixtures remain to test their configured behavior.

Automatic journal snapshot attempts and continuation publication use a fifteen-second budget rather than the delivery lifetime. Continuation archive/manifest writes, journal/signal purges, suspension append and handoff share that deadline. Timeout retains the recorded frame and follows ordinary NAK/release/redelivery recovery. Frame and spilled-result Put/Get calls during handler execution receive separate fifteen-second request contexts; user effects keep their original lifetime. Missing result replies use ordinary retry classification. Confirmed frames remain reusable, while unrecorded effects can execute again with a stable RunOnce key.

When a ready suspended wait remains at the same journal tail, its scanner wakeup message ID changes every ten seconds. Scans within a window deduplicate, while the next window can supply a fresh run if a prior wakeup was consumed during lease contention and its nak was lost. The scanner stops reenqueuing after the journal advances or the wait is no longer ready; the mixed fault gate still measures under 30 seconds from its last enabling event.

The sustained R5 worker SIGKILL row is
`TestFiveContainerMixedWorkerKilledEveryFiveSeconds` (`worker_kill` in
`tier3-mixed-leaders`). Five actual worker processes run the shared mixed workload;
seeded kills occupy five-second slots, followed by replacement generations.
A500ms acquired-delivery handoff confirms held targets when available; other
selections explicitly retain the no-new-acquisition outcome. At least one held
target kill is required. Raw enabling-event p99, histories, invariants and final
physical queue drain remain required. Original per-process dispatch/fencing files
and surviving final counter snapshots are checked; interrupted tails and killed
processes' missing final counters remain explicit. Smoke does not establish the
ten-minute row or full24h matrix, and cannot certify complete hard-kill attribution.

The R5 pause-past-lease row is
`TestFiveContainerMixedWorkerPausedFortyFiveSeconds` (`worker_pause` in CI).
Five actual worker processes run the shared mixed workload; at+5s and every
minute a seeded active process is SIGSTOPped for45s and resumed with SIGCONT.
The fixture confirms stopped/running OS states and retains actual KV ownership
snapshots while stopped. After resume, typed fencing must match a retained lease
key and epoch. The same five PIDs/generations survive, and all final fencing
counters are cross-checked. Raw p99, histories, retained invariants and physical
drain remain required. A35s smoke performs the full45s pause; ten minutes require
ten pauses. This does not replace the full24h mixed fault matrix.

The retained fencing reviewer joins exact worker/run/delivery fetch records to
journal terminal timestamps and confirmed fault intervals. It distinguishes
already-terminal duplicate fetches from completion during an original delivery,
while an owner is stopped, or after fencing. Later invocation ack observations
are kept separate from the exact delivery; local ack success alone is not broker
commit evidence. This review supplements history/invariant/drain checks and does
not infer server causes or full-release acceptance. Missing or ambiguous original
fetch/terminal evidence fails the review rather than becoming a causal claim.

The R5 worker reply-isolation row is
`TestFiveContainerMixedWorkerRepliesIsolatedFortyFiveSeconds` (`worker_isolation`
in CI). Five actual workers are pinned through separate proxies. At+5s and once
per minute a selected acquired delivery remains held while its journal is read;
only an unfinished invocation is admitted. The original nonterminal prefix is
retained. Replies are held for45s while requests still reach the server; relay
counters confirm asymmetry and reject overflow. A fresh successful worker PING
and resumed replies confirm heal. Typed fencing must match the selected delivery;
its final journal preserves the cut prefix and completes after the cut. All
mixed histories/invariants/raw p99/physical drain and final process counter checks
remain required. A terminal duplicate delivery is separate coverage and cannot
clear this row. Ten minutes require ten actual reply holds; smoke never clears
the full24-hour matrix.

The sustained server-clock-ahead and server-clock-behind row scaffolding combines
one actual server at±60s with thirty-second journal-leader SIGKILL/restart cuts.
It retains controller-bracketed monitoring reads for all five actual clocks at
startup and before/after every cut, plus confirmed journal-leader metadata and
ordered process-removal/restart observations. The artifact checker rejects
missing/unshifted clocks,wrong leader scope and reversed process timelines.
These rows now use an independent unshifted controller audit. Append starts
bound journal commits below; successful acknowledgements or independently
matched journal receipts bound them above. Unknown returns never provide commit
upper bounds. Root SDK calls and parent child requests bound enqueue; external
signal calls and child terminal windows bound readiness. Runtime child-signal
publication is conservatively bounded by invocation creation when no observed
SDK call exists. Timer deadlines join each retained `fire_at` to the successful
SDK clock lookup that created it. Completion must begin after that lookup's
latest possible deadline; uncertainty cannot be accepted as proof of no early
completion. Latency p99 uses conservative delay bounds with the strict30s gate.
Raw broker timestamps remain retained data and never supply mixed-clock latency.
The artifact checker reconstructs windows,sample origins,p99 and timer coverage.
Terminal windows overlapping a fetch or fencing event retain an uncertain
classification; they are never presented as exact commit timestamps. Clock-role admission now places both `WF_RUN` and `WF_JRN` on the skewed peer
before workload start and each cut. The fault kills that peer and holds it down
until both streams elect unshifted replacements, then restarts its retained
store and requires full R5 recovery. Initial,before,replacement and after role
metadata plus actual shifted timer-clock lookups are required. Peer-only smokes
whose timer lookups stayed on unshifted leaders are retained as limited evidence
and cannot clear this strengthened requirement. This admission does not prove
every in-flight timer/effect/continuation cut combination; those remain open.
Pending timer cut preparation now includes a deterministic candidate selector
with shifted clock-origin and observed suspended-prefix checks. A candidate
alone cannot admit a fault: refresh the actual retained tail, match its exact
sequence/entry, confirm removal before the earliest conservative duration
boundary, and corroborate the prefix in the final audit. The fixture and artifact
guard implement these checks, including separate canonical creation and shifted
native hint proofs; see [canonical admission evidence](scale/canonical-timer-admission-2026-10-02/).
Admitted canonical native recovery and the full cut combinations remain open.

No sustained clock
row is verified by the focused controller contract; existing focused clock proofs and the full
24-hour matrix remain separate requirements.

The R5 clock rows can opt into the shared canonical clock with
`WF_TIER3_COMMON_CLOCK=1` (workflow input `common_timer_clock`). Five uniquely
tagged R1 memory probes bind to five physical servers. Workers and suspended
repair share one provider with the configured one-skewed-peer assumption;
canonical creation observations and fresh shifted native hints are retained
separately. `independent-clock.json` preserves exact topology and startup bounds,
and the artifact guard requires matching node tags and canonical domains on all
retained timer requests. Combine this with `WF_TIER3_CLOCK_TIMER_CUT=1` for
removal before the original request-start-plus-duration boundary. The existing
early-completion and strict30s latency gates apply. Historical legacy-clock
failures remain open until new admitted native evidence passes.

**The "done" bar for a release**

The Tier 1 timer transport also models explicit leader wall-clock source
transitions with controller-clock delivery observations. Seeded ±60s traces
characterize retained absolute-deadline sensitivity; these transport assumptions
do not certify NATS or production-worker clock tolerance. See the
[timer clock model evidence](scale/timer-clock-model-2026-10-01/). The dispatch model
also characterizes consumer pending-deadline source transitions for ACK-wait,
progress and delayed NAK. Six legacy hypothesis traces remain replayable.
A focused pinned-NATS R5 durable-consumer test now verifies that initial restored
pending timestamps derive from stored messages, while progress and delayed NAK
replace them with consumer time. Eighteen new seeded combinations model these
separate clock sources; six durable native cases calibrate the contract, not the
whole combination matrix. See [native contract and replay evidence](scale/consumer-clock-native-contract-2026-10-02/).
Neither characterization clears the historical ahead physical-drain failure.

Fallback scan capacity is part of the fault-latency configuration. The retained
clock-row failure showed that eight invocation sequences per second did not
visit three newly suspended waits before admission canceled. Clock rows now use
256 sequences per100ms, with the actual policy retained in artifacts. Seeded
production scan/cursor cases reproduce the slow page and configured repair for
populations up to3000; see [scan capacity evidence](scale/suspended-clock-scan-capacity-2026-10-02/).
Keep the original admission and latency gates. Ten-minute capacity does not
prove24-hour capacity: measure sweep time and RPC pressure at that population,
including completed retained invocations, before claiming the full matrix.

1. Tier 1: 100 000 seeds clean.
2. Tier 2: 200 consecutive seeds clean across the whole matrix.
3. Tier 3: 24 h soak with the full matrix, zero invariant violations, zero stalls, and a written explanation for every fencing event and every reconciler re-enqueue (they are expected; unexplained ones mean a bug the checkers missed).
4. Every bug found in tiers 2 or 3 during the cycle has a tier 1 reproduction added.
5. A chaos run with a deliberately introduced bug in each of the six invariants (a mutation test: disable the CAS header, skip the lease, reverse the purge order, drop the determinism guard, remove `Nats-Msg-Id`, skip the reconciler) is caught by the checkers. If a mutation survives, the test suite is not done.

## Edge case catalogue

One row per failure mode that cuts across phases, with the mechanism that handles it and the test that proves it. The per-phase lists above hold the phase-local cases; this table is the cross-cutting index a reviewer can audit.

| Failure mode | Where it bites | Mechanism | Proving test |
| --- | --- | --- | --- |
| Publish acked on server, ack lost to client | Every append and publish | Re-read tail / `Nats-Msg-Id` / `ErrAlreadyStarted` | Phase 1 dropped-ack proxy; phase 2 `ErrUnknown` ×1 000 |
| Two workers believe they own one invocation | Dispatch | Lease epoch + CAS append (defence in depth) | Phase 3 pause-past-lease; lease-disabled debug build still safe |
| Worker paused (GC, VM stall) longer than lease | Dispatch, timers | Fencing on next append | Phase 3 45 s pause |
| Crash between two dependent writes | Start, sleep, signal, purge | Ordered writes + durable repair scanners | Phase 5 kill-between ×200; phase 7 pipeline boundaries |
| Duplicate wakeup | Timers, signals, reconciler | Idempotent wakeup; nak-with-delay when lease held | Phase 5 cancel-then-fire; phase 7 double leader |
| Effect executed twice | SDK steps | At-least-once effect, exactly-once recorded outcome; `RunOnce` key | Phase 4 41 kill points |
| Code changed under running invocation | SDK | Determinism guard + `ctx.Version` | Phase 4 renamed step 7 |
| Reused id after purge sees old data | Retention | Strict purge order, tombstone | Phase 7 10 000 reuse |
| Stream eviction of live data | `WF_JRN`, `WF_RUN` | `Discard=new`, `MaxAge=0` on `WF_RUN`, config assertions | Phase 2 eviction; phase 7 misconfig |
| Payload over `max_payload` | Inputs, results, signals | Object Store spill | 5 MiB tests in phases 1, 2, 6 |
| Raft leader change mid-operation | Everything | Sequence numbers are stream-global; retries | Phase 0 baseline; leader kill rows of chaos matrix |
| Mixed server versions | Batch publish, scheduling | Version check, fail closed, fallbacks | Rolling-upgrade row of chaos matrix |
| Clock skew | Timers | Server timestamps for timer completion | Phase 5 ±5 s; matrix ±60 s |
| Subject cardinality | `WF_INV`, `WF_JRN` | Purge pipeline; measured memory ceiling | Phase 1 10 M subject measurement |
| Hot partition | Dispatch | Known limit in v1; isolation of other partitions | Phase 3 hot-tenant test |
| Reconciler too aggressive / too quiet | Liveness | Idempotent wakeups; liveness bound in every tier | Phase 7 control runs (on/off) |
| Poison invocation | Dispatch | Attempt counter in journal, terminal `Failed` | Phase 3 count across restarts |
| Journal gap after purge | Replay | `ErrJournalGap`, refuse to run | Phase 4 corrupted fixture |

## Open risks and what to measure in the first two weeks

Three numbers decide whether this design survives contact with production, and all three can be measured with the phase 0 fixture before any SDK code exists.

1. **Per-subject memory on the server.** `WF_INV` and `WF_JRN` each hold one subject per invocation. Fill a 3-node cluster with 1 M, 5 M and 10 M subjects and record RSS per node and stream-info latency. If 10 M costs more than a few GB per node, the retention window must be short or `WF_INV` needs a different design (a KV with TTL instead of a stream).
2. **CAS append throughput on one subject and across many.** `Nats-Expected-Last-Subject-Sequence` is checked by the stream leader; measure appends/s at `Replicas=3` with file storage for one hot invocation and for 10 000 concurrent ones. If a single invocation caps below \~500 appends/s, step-heavy workflows need batching of `StepRequested`/`StepCompleted` pairs.
3. **Scheduled message behaviour at volume.** Publish 1 M scheduled messages due over 24 h, restart the cluster twice, and confirm they all fire within tolerance. Use the [native timer volume runner](timer-volume.md) and offline observation verifier; its shortened smoke is excluded from this gate. Receipt evidence must be synced before acknowledgment so interruption cannot erase already acknowledged timestamps; offline receipt recovery must keep an interrupted run uncertified. This feature is new in 2.12 and its interaction with stream limits, replication and restarts is the least battle-tested part of the whole stack.

Other risks, in rough order of how much they would change the plan:

- **The in-memory JetStream model for tier 1 drifts from the real server.** Mitigation is procedural: no tier 2 or 3 bug is closed without a tier 1 reproduction.
- **Static partitioning.** N=64 fixed partitions means rebalancing is a manual operation in v1. Acceptable for a first release; the lease makes a later dynamic scheme safe to introduce.
- **Go-only SDK.** Multi-language SDKs are where these projects die. Keep the journal format and the step protocol language-neutral (protobuf, documented recovery table) from day one so a second SDK is a port, not a redesign.

The [version-1 journal and recovery contract](journal-protocol.md) now includes a protobuf interchange schema, lossless Go adapters and independently generated Python codec vectors. Production writes default to JSON; versioned protobuf writes and mixed-format readers are now supported. The protocol document records the reader-first rollout and focused recovery/compaction evidence. Automatic negotiation, historical rewriting and full protobuf rolling/chaos acceptance remain open. A second SDK is future scope under the original Go-only SDK plan, not an added release gate.
- **Operational coupling to the NATS cluster's health.** A JetStream cluster that loses quorum stalls every workflow. This is the same trade Temporal makes with its database; document it and test the full-restart rows of the matrix.

The reconciler is a first-class part of v1. Atomic batches cannot span this runtime's separate invocation, run, and signal streams, so repair scanners close the unavoidable crash windows between their dependent writes.


### High-cardinality live traffic measurement

The standalone `cmd/wf-scale` runner supports `-live-workflows` and
`-live-input-bytes` after each `-counts` checkpoint. Retain an explicit fresh
root, the report and raw live-cohort audits. Run both inline and spilled payload
cohorts at the target cardinalities, checking cross-node immutable results,
input hashes, retained journal/state invariants, exact aggregate subject/message
counts, queue drain, and process RSS. Opaque background capacity records do not
count as completed workflow evidence. A small runner smoke does not clear the
10-million-subject live traffic requirement.

The fresh10M spilled-input live cohort now passes at160cf8d:1000 production
workflows with1.1MB inputs, immutable cross-node outcomes, exact retained counts
and physical drain. [Raw measurement and offline verification](scale/live-cardinality-spilled-10m-2026-10-02/)
record p99/max1.696/2.801s and post-live RSS3879–3952MiB per node. This closes that
measurement, not10M active executions or the chaos/soak requirements.

Tier3 completed-cohort audits now use the existing high-water checker on an
independent reader so checkpoints do not stop production of positive pending
waits for clock cuts. Every tenth-batch checkpoint must finish with exact cohort
counts; final whole-retained-state checking remains mandatory. Future CI also
requires all checkpoint artifacts. See [checks and scope](scale/tier3-independent-checkpoints-2026-10-02/).

### Admitted server-clock timer workload profile

When `WF_TIER3_CLOCK_TIMER_CUT=1`, all eight timer waits are two seconds. Giving
only the first wait two seconds allowed a healthy first publication just before
leader preference, followed by shifted 250ms waits that could never satisfy the
750ms actual-removal lead. The [retained replay and deterministic selector regression](scale/clock-admission-wait-profile-2026-10-02/)
record that failure and the interval correction. Ordinary non-admitted clock
rows retain eight 250ms waits. Admission, clock-origin/publication proof, duration,
latency, retained-state and drain gates are unchanged; fresh full native evidence
remains required for the corrected admitted profile.

### Dispatch acknowledgement outcome

Workers now use a parent-bound two-second `DoubleAck` for completed and
terminal-held deliveries and record `dispatch_ack_confirmed` operation timings.
A nil result establishes a server reply; an error remains ambiguous. It does not
prove physical WorkQueue deletion, which is checked separately. The
[held-reply R3 contract](scale/confirmed-dispatch-ack-2026-10-02/) verifies committed
ACK with lost confirmation and successful retry. This improves provenance over
local asynchronous ACK returns; it does not close the old ahead-clock drain miss.


### R5 consecutive-seed campaigns

`tier3-mixed-journal.yml` accepts `seeds=1|20|200` for one selected R5 row,
including both admitted common-clock rows. Each independent job receives its
actual `FAULT_SEED`, requires the executed seed to match, and uploads a distinct
row/seed artifact. Four jobs run concurrently and failures do not cancel other
seeds. Keep `duration=10m`, `common_timer_clock=true` and
`clock_timer_cut=true` for sustained admitted clock campaigns. A campaign clears
only its complete requested seed range at the recorded source after all original
artifacts are verified; single-row campaigns do not replace the full-matrix or
24-hour release gates. The 35s duration remains smoke evidence only.

Use `scripts/check-tier3-campaign.py --metadata <run-with-jobs.json> --artifacts
<download-root> --row <row> --seeds <count> --duration 10m --output <report.json>`
to independently verify a terminal campaign. Add `--require-admitted-clock`
for clock campaigns. The command requires every seed's original events and
fault artifacts, current verification matching the uploaded report, and the
complete successful job set. A green summary without these inputs is insufficient.

### Clock cut exit observation

The Docker harness observes source exit separately from automatic container-name
cleanup. A successful exact-name listing of `exited`, `dead` or absence gives a
controller-clock upper bound on exit; running/removing states and failed listings
cannot prove it. The admitted cut must still precede the earliest duration
boundary, and restart waits for name cleanup. Receipts retain both observations
and bind node/container identity to the independent clock source and restart.
Historical artifacts keep their original conservative cleanup timestamp. See
[source-exit evidence and limits](scale/docker-source-exit-2026-10-02/).

### Native timer hint retirement after completion

The production timer and suspended-wait repair loops retire native schedule
hints after reading a durable Completed or Failed journal. Cleanup matches the
invocation generation and deletes an observed stream sequence, preserving active
invocations and concurrent replacements. Recurring scans retry ambiguous deletes
and catch late hints. This keeps physically retained future hints from extending
the completed-cohort drain beyond the unchanged30s target after a scheduling-clock
leader change. Domain-aware repair still owns due-time correctness and liveness.
Both scanner paths, lost/delete replies, dry runs, generation isolation and
sequence replacement have seeded and native contracts; sustained admitted native
row validation remains required. See [retirement evidence](scale/native-timer-retirement-2026-10-02/).


### Additional qualification boundaries (2026-10-02)

Near the journal cap, combine continuation restoration with a held production
lease, actual worker SIGKILL, and a full retained-store cluster outage/restart.
Cut after signal consumption, step completion, and terminal failure; require the
original global-index prefix, one immutable ErrTooLong outcome, no effect beyond
the budget, frame-only recovery, and matching offline continuation replay.
Exercise private16/20 budgets for iteration and the unchanged production100000
default for qualification. A compiled restored-suffix budget control must fail
semantically. Local budget20 passes all cuts; production-cap qualification and
Tier1 modeling of these combined fault boundaries remain required.

The million-timer campaign must certify physical stream/consumer drain after
all receipts, not infer drain from delivery counts or default report fields.
The original24-hour attempt delivered all1M but failed its final deadline;
retain that failed verdict and diagnose final drain before another long run.


### Delivered native source retention in deterministic simulation

Model target delivery separately from physical source retirement, including
retention during delivery and source restoration after delivery. Execute production
TimerScan/SuspendedScan cleanup from terminal journal evidence, preserve active
work and newer generations, and inject dropped or lost delete replies. Require
exact trace replay, a disk pin, and compiled controls detecting both the old
delivery-implies-absence assumption and deletion across generations.
The119th workload passes local1,000-seed/full-suite validation; current119-workload
100k qualification and real backend persistence-cause isolation remain open.


### Physical replica drain qualification

Final timer-volume certification must retain local WF_RUN message and consumer
pending counts from every physical replica as well as leader metadata. Require
all replicas empty after completed receipts; Raft-current metadata is not a
substitute for matching local contents. Retain explicit observed counts and
errors. Copied million-timer stores expose retained sources behind a fully
stamped empty scheduling index; rebuilding copied indexes emits duplicate
targets. Preserve the original failed verdict and require backend recovery
isolation before any index repair or new million-timer release claim.

### Deterministic combined continuation-limit takeover

Combine the production worker's near-cap continuation recovery with a stopped
owner whose lease remains held and an outage preserving committed transport
state. Require three cut positions, nonzero continuation index/step offsets,
virtual lease-expiry boundaries, higher replacement fencing epochs for successor
appends, immutable cut prefixes and terminal results, zero forbidden effects,
frame-only recovery, offline staged replay and drained original dispatch.
Keep the real-cluster combined proof as an independent gate. The 120th Tier 1
workload implements 27 cut/budget/heal combinations; focused race/replay and the
compiled suffix-budget negative pass. Full final-source qualification is open.


The real combined continuation-limit fixture is independently qualified at the
unchanged production100000 cap by [run37045827952](scale/continuation-limit-combined-2026-10-02/prompt-effect-production-cap/).
All three worker-kill/retained-server-restart cuts preserve raw prefixes and
recover below30s withTTL12s. The corrected compiled suffix-budget negative
fails on one actual forbidden effect. This clears that fixture gate; current
full Tier1 release coverage, complete fault matrices and24h soak remain required.


### Single-row 24-hour verification support

The existing five-container mixed-row Go harness accepts
`WF_TIER3_MATRIX_DURATION=24h`. Its offline row verifier now also accepts
`--duration 24h`, requiring actual named-test/package completion, a matching
24-hour result and the full row-specific fault count, with the existing
audit, latency and raw artifact checks. Short elapsed time, a ten-minute claim
and missing fault cuts are rejected. This enables long-run evidence collection;
a single row retains `clears_full_tier3_release=false`. The full-matrix
24-hour requirement remains unchanged. The hosted workflow still selects
35-second smoke or ten-minute rows; adding verifier support is not a soak pass.


### Complete five-container sustained row campaigns

`tier3-mixed-leaders` now accepts `row=all` for all fourteen implemented R5
fault variants at one revision, with 1,20 or200 consecutive seeds. All-row
campaigns shard at twelve seeds per job: the200-seed campaign uses238 jobs,
within the hosted256-job limit. Each seed retains separate raw events, row
reports, checkpoint audits, event explanations and fencing reviews. Clock proof
flags apply to both clock rows; focused non-clock requests with those flags
are rejected. Focused single-row campaign naming/layout remains compatible.

`check-tier3-full-matrix.py` requires every planned terminal job and exact
checkout/seed header, regenerates every row report from raw artifacts, compares
uploaded report bytes, and regenerates event/fencing explanations. Optional
admitted-clock verification requires all cut/probe evidence on both clock rows.
A ten-minute full-row campaign still reports `clears_full_tier3_release=false`:
complete fault coverage and the required24-hour full-matrix soak are separate
requirements. Dispatch and planner/guard tests do not establish a campaign pass.


### Million-population native timer diagnostic

The manual `native-million-diagnostic` workflow runs the existing volume tool
with1,000,000 schedules,64 publishers, a15-minute loading runway and ten minutes
of deadlines. It retains both all-server SIGKILL cuts and mandatory all-replica
physical drain. Diagnostic raw lateness limits are30s p99/60s maximum; raw
measurements remain in the report. Offline verification explicitly uses
`-allow-smoke`; this profile cannot clear the original24h/2s/30s release gate.

This probes full population and production persistence cadence beyond the
earlier100k/90s diagnostic. It preserves all captured source hashes, binary,
receipt ledger, observations, logs and physical stores in a lossless archive,
with SHA256 member readback before publication, even after a campaign failure.
A passing diagnostic would not establish the original missed-retirement cause.


### Original matrix requirements beyond the initial fourteen R5 rows

The original chaos table also requires worker clock skew and rolling server
upgrade. The initial fourteen-row five-container registry does not contain
those two rows; its complete campaign is implemented-row coverage, not full
original-matrix completion. Tier2 has both rows. Their R5 ports remain required
before the full24-hour release gate can be claimed.

`StartRollingUpgradeDockerCluster` now starts every peer on an explicitly
supplied old static server executable. `UpgradeNode` changes only the selected
peer's binary and keeps its file-store binding. The existing mixed-version
constructor still starts only node0 old. A manual docker-rolling-upgrade
contract checks actual five-peer2.11.17 startup, all five transitions to2.15.0,
retained R5 message bytes and physical local counts after each transition. Its
compiled single-old-peer control must fail on the initial version observation.
This constructor/retention contract does not substitute for the mixed-workload
rolling-upgrade row, fail-closed feature checks or24-hour matrix soak.

### Lost-release fencing regression (121st Tier1 workload)

R5 worker-pause run37058644370 exposes a missing observation: execution retries,
Release detects expired ownership, Cleanup returns nil, and no FencingEvent is
recorded. Reproduce through production Worker/Lease/Journal paths with seeded
virtual TTL expiry, missing/successor retained keys and preexisting execution
fencing. Pin seed3; require one event/counter per delivery, original identity and
epoch, successor preservation, unchanged Started journal and unacked dispatch.
Record the first lost ownership before cleanup, with per-delivery deduplication.
The pause fault must wait for an actual matching paused-lease fencing record.
Keep the original artifact checker rejection and add an executed compiled
release-observation omission control. Focused model proof does not clear current
121-workload full-suite/100k or real R5 pause qualification. The prior120 graph
100k gate remains independently accepted atad37bfc.

### R5 rolling-upgrade row preparation

The original rolling-upgrade requirement now has a sustained six-cell R5 row
implementation: all five peers start2.11.17, each peer is upgraded exactly once
in seeded order across10m (or24h), with fallback timer poller continuing during
cuts. Each before/after deployment proof pins five unique actual server
identities/versions, R5 file readiness for run/timer streams, retained fallback
selection and semantic explicit-native rejection. A35s fixture smoke upgrades
one peer and cannot qualify a full rolling upgrade. Preserve complete physical
stores and all evidence in verified archives. Port is prepared, pending actual
mixed-row qualification; the independent37-message five-peer constructor
contract is accepted. Worker-clock skew remains an original R5 port gap.

The implemented registry is now15 rows. Full campaigns use13-seed shards and
340m job budgets:15xceil(200/13)=240 jobs, leaving capacity for the remaining
worker-clock row (16x16=256). Existing14-row/12-seed evidence retains its source
and original scope. No new registry size clears the full original matrix or24h.
