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
- Test: 100 000 distinct ids, then count subjects in `WF_INV` equals 100 000 and `WF_RUN` message count equals 100 000. Repeat with `PartitionNodes` toggling every 200 ms. Keep route toggles active until every Start producer finishes, rather than stopping after a fixed initial twelve ticks. The [continuous-fault fixture and semantic control](scale/partition-start-fault-duration-2026-10-04/) retain route timestamps, final confirmed heal and optional original stores; lifecycle controls alone do not qualify the full count proof. The [complete 100,000-ID R3 execution](scale/partition-start-fault-duration-2026-10-04/full100000/) at `3b859aa` now qualifies this count case:44.17 s, all three100,000 counts and206 changes through99,994 completions, with actual executable/captured sources/all375 original broker files preserved. Full Phase1 and matrix/24h gates remain separate.
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
  The default-size `TestThousandJournalNetworkLostAckRecoveries` now applies
  actual TCP faults to all 1,000 attempts, with committed/absent branches and
  a midpoint real R3 journal-leader stop/restart. The normal retained-binary
  run at `eacff61` is independently accepted, including all raw peer receipts
  and complete wire transcript. [Full evidence and scope](scale/journal-network-acks-2026-10-03/full/)
  closes this specific gate; it does not replace final-source matrix/24h gates.
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

- Test (the one that matters): 200 invocations, each a loop of 50 steps that increment a counter in the journal. 6 workers. Every 2 s one of: kill a worker, pause a worker for 45 s (longer than the lease), partition a worker from the cluster. After 5 min: every invocation completes with counter exactly 50, and the integrity checker shows I2 holds on every journal. Any journal with two epochs interleaved fails the run. The opt-in `TestPhaseThreeTwoHundredCountersWithRepeatedWorkerFaults` now implements this exact 200/50/6 cohort, overlapping 45-second holds while continuing two-second fault choices. [Profile scope and retained originals](scale/phase3-repeated-worker-faults-2026-10-04/) distinguish prepared code from actual qualification. The [complete seed1 original cohort](scale/phase3-repeated-worker-faults-2026-10-04/full-cohort/) at `24cf857` now qualifies this case:200 counters returning50 in141.438603 s,70 faults including six45-second pauses and six45-second network cuts,20,400 verified raw entries and all200 independent SDK replays. Actual executables/captured sources/all387 original store files remain preserved; full Phase3 and matrix/24h requirements remain separate.
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
- **Outer handler or named continuation blocks outside a step and ignores cancellation.** Stop waiting at the handler boundary when delivery/handler context is cancelled; leave its SDK/journal buffers owned by that goroutine and discard any late result. Every SDK append must reject cancellation before accessing the durable append closure. A retained cancel is drained on redelivery. Outer Goexit follows the bounded panic-attempt policy. Prove successor completion, unchanged accepted journal after a late SDK call, worker shutdown and native bounded Goexit attempts; ignored external effects still require idempotency. Focused race/model/R3 Goexit controls pass. Focused real R3 race lease-expiry controls for both an ordinary handler and named continuation now qualify at `fb1e515`: real productionTTL12s/AckWait13s takeover, old heartbeat CAS fencing, shutdown with ignored handler held, unchanged journal/no late effect and all-three public local queue drain. [Complete native evidence](scale/outer-handler-lease-expiry-2026-10-06/). Other network/process/clock/checkpoint cut combinations and full release qualification remain open.
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
  Qualification on 2026-10-03: all six sustained components are independently
  accepted against reference `06762b6`, with identical Go/fixture/module inputs,
  workflow and selected mutation definitions. The corrected Start retry replaces
  a harness anchor failure; the failed parent campaign remains rejected.
  [Combined raw review and source-equivalence evidence](scale/sustained-component-verifier-2026-10-03/accepted/)
  clears this gate only; full 200-seed matrix and 24-hour soak remain open.
- CI: first gate a fixed regression corpus plus 1,000 seeded schedules per commit; raise to 10,000 per commit once measured on CI hardware within a few minutes. Record seeds, trace paths, model version, steps/s, and virtual-time coverage. Keep 100,000 clean seeds and all known regression traces as the release gate. A timeout, unexplained skipped action, or model/real contract mismatch fails the gate rather than counting as a clean seed.
  The 121-workload graph's normal100k gate at `9ecc37c` is independently
  accepted, including all 391 pins and the actual retained binary. A later
  snapshot first-lookup timeout cause fix changes production source and requires
  fresh normal100k/race1k qualification; the previous result remains historical.
  The corrected `283ba32` full race1k is now independently accepted: all 121
  workloads complete 121,000 bodies; actual race executable, all 1,160 captured
  source hashes and complete raw inventories verify. Normal100k at that same
  production source now independently qualifies all 12,100,000 bodies: all 391
  pins, 175 top-level tests and 1,160 captured source hashes verify, with the
  actual normal executable preserved. A 686-input ledger establishes identical
  current runtime/simulation/Tier1 producer bytes.
  [Complete corrected-source normal proof](scale/snapshot-timeout-cause-2026-10-04/hosted-normal100k/)
  and the [Corrected-source race proof](scale/snapshot-timeout-cause-2026-10-04/hosted-race/)
  do not qualify failed real-cluster cases or the remaining full release gates.

  [Diagnostic correction and unchanged failure bounds](scale/snapshot-timeout-cause-2026-10-04/)
  do not qualify the failed combined continuation/promise restart case.
  A 686-file ledger establishes unchanged runtime/model/Tier1 producer inputs
  through reference `9c5fce3`; later integration-only fixtures are excluded.
  The full race1k at the same tested source is separately accepted.
  [Complete normal100k originals and qualification scope](scale/tombstone-marker-drain-2026-10-03/hosted-full100k/)
  does not replace the real full-matrix or 24-hour gates. Tier3 permits five
  real VMs or five containers; a separate cross-VM run is optional deployment
  validation, not an additional release requirement in the supplied plan.

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

`scripts/run-tier3-soak.py --root /external/fresh-root --row journal` now
provides a local24h producer. It creates an isolated committed sparse checkout,
retains the actual race binary/build settings and source hashes, records the live
test PID, executes with24h20m Go timeout, and applies row/checkpoint/explanation/
fencing verifiers. Clock rows require admitted timer cuts and independent common
clock probes; explicit rolling gap/shutdown profiles retain their proof gates.
All stopped-run originals, including physical stores and compiled checkout, are
archived with every-member readback. Interrupted live children retain their root
without a final archive claim. A single row cannot clear the full matrix/soak.
The journal seed1/35s race producer smoke at65f4b7f passes with complete
archive/source/binary/report review and five consistently hashed negative
controls. Other producer profiles and all actual24h rows remain unqualified.
Review an archive with `scripts/review-tier3-soak.py`; it regenerates the raw
row/explanation/fencing reports using recorded source scripts and verifies the
compiled checkout against Git. Hashing stores does not independently reopen them.



### Complete five-container sustained row campaigns

`tier3-mixed-leaders` now accepts `row=all` for all sixteen implemented R5
fault variants at one revision, with 1,20 or200 consecutive seeds. All-row
campaigns shard at thirteen seeds per job: the current 16-row200-seed campaign uses256 jobs,
within the hosted256-job limit. Each seed retains separate raw events, row
reports, checkpoint audits, event explanations and fencing reviews. Clock proof
flags apply to both clock rows; focused non-clock requests with those flags
are rejected. Focused single-row campaign naming/layout remains compatible.

`check-tier3-full-matrix.py` requires every planned terminal job and exact
checkout/seed header, regenerates every row report from raw artifacts, compares
uploaded report bytes, and regenerates event/fencing explanations. Optional
admitted-clock verification requires all cut/probe evidence on both clock rows.
For the current forced-gap SIGKILL campaign, also pass
`--require-upgrade-start-gap --expected-upgrade-shutdown sigkill`; the reviewer
regenerates production Start-scan progress proof and rejects missing gaps or
a substituted shutdown mode. Use `ldm` for that independently requested profile.
A ten-minute full-row campaign still reports `clears_full_tier3_release=false`:
complete fault coverage and the required24-hour full-matrix soak are separate
requirements. Dispatch and planner/guard tests do not establish a campaign pass.

`check-tier3-matrix-shard.py` supplements that whole-campaign review with
complete successful terminal-shard checks while a parent is active or failed.
It requires exact job/run/artifact/source binding, every requested ten-minute
seed, raw report/event/fencing regeneration and applicable complete source
ledgers. Missing cases and failed jobs do not count. Parent, whole-row and full
release qualification remain false; it cannot convert a failed parent into a
passed campaign. [Reviewer calibration and scope](scale/tier3-shard-reviewer-2026-10-03/)
retain the actual prior-input checks and their provenance limits.

The corrected worker-clock capture has a full ten-minute seed 1 qualification
at `070dd95`: all 21 snapshots prove five current replicas, complete original
archives/source ledgers verify and all three independent history models pass.
Old full campaign 37157123048 is cancelled after three repeated capture
rejections and no successful row jobs. Replacement 37164231641 at `79915ca`
retains all 16×200 ten-minute executions and requested upgrade/clock profiles.
[Qualification and replacement proof](scale/worker-clock-recovery-2026-10-04/)
clear only the corrected individual case; neither dispatch nor cancellation
clears the whole matrix or 24-hour release requirement.

Replacement worker-clock shard 14–26 then fails at seed 15's retained-audit
deadline; the parent cannot qualify. Missing state, server cause and audit
capacity remain unconfirmed. All original archive/source hashes verify. Retain
each checkpoint attempt's timing/deadline/partial report and log the primary
failure before cancellation masks it. Keep the existing three 20-second attempts
inside 60 seconds and all workload gates unchanged while investigating. Focused
ten-minute diagnostic 37166976657 at `c2e8c01` qualifies seed 15 only after
independent raw/source/clock and three-model review: 2,884 invocations,
31,915 entries and 19 faults. All ten checkpoints pass on their first attempt;
the last takes 18.782 seconds against the unchanged 20-second attempt bound.
The successful case does not establish the original failure cause or repair
the failed full parent. The shard reviewer accepts the actual flat focused-store
archive only with exact single-seed original-store identity; raw/range archives
still require their nested fixture layout. [Accepted diagnostic evidence](scale/worker-clock-checkpoint-2026-10-04/accepted-seed-15/)
defines this limit. Other campaign jobs remain useful individual evidence.
Complete worker-clock shard 27–39 at `79915ca` is now independently accepted:
39,452 invocations, 437,316 entries, 247 faults and all three production models
over 50,724 operations. All captured source/clock/checkpoint and 51,520 canonical
member hashes verify; actual workload/model executables and full raw proofs are
retained. [Thirteen-seed qualification and physical-archive availability limits](scale/current-tier3-clock-2026-10-04/worker-clock-27-39/)
keep the failed full parent, full 200-seed row/matrix and actual 24-hour gate open.
Complete shard 40–52 at that same executed source subsequently qualifies with
37,632 invocations, 416,582 entries, 247 faults and all three independent models
for 48,384 operations. Exact recorded executable hashes match previously
verified retained bytes; associated SDK material is explicitly reconstructed
from their provider. New physical-store artifact is referenced only, not verified.
[Raw-source/model proof and explicit material provenance](scale/current-tier3-clock-2026-10-04/worker-clock-40-52/)
extend same-source clock coverage to 26 seeds 27–52 without promoting the failed
parent, complete row/matrix or original 24-hour gate.
Complete shard 66–78 at the same source also qualifies: 39,984 invocations,
443,511 entries, 247 faults and 51,408 independent model operations. All captured
source/clock/cohort proofs and exact provider executable hashes verify; new
physical stores remain reference-only. [Complete raw/model proof and scope](scale/current-tier3-clock-2026-10-04/worker-clock-66-78/)
extends accepted coverage to 39 seeds (27–52 and 66–78), 117,068 invocations,
1,297,409 entries, 741 faults and 150,516 history operations. The failed parent,
missing ranges, full row/matrix and actual 24-hour gate remain open.
Complete shard79–91 at the same executed source now qualifies:40,012 invocations,
443,742 entries,247 faults and51,444 independent model operations. All759 source
inputs,273 clock proofs,1,365 broker messages and137 cohort audits verify;
actual provider-matched workload and model executables remain in the compact
proof. New stores are reference-only, not downloaded/verified/reopened.
[Complete raw/model evidence](scale/current-tier3-clock-2026-10-04/worker-clock-79-91/)
extends accepted coverage to52 seeds (27–52 and66–91),157,080 invocations,
1,741,151 entries,988 faults and201,960 history operations; the failed parent,
missing ranges, full row/matrix and actual24h gate remain open.
Complete shard 92–104 at the same executed source qualifies with 37,996 invocations,
420,849 entries, 247 faults and 48,852 independent history operations. All 759
selected source inputs, 273 clock proofs, 1,365 broker messages and 130 cohort
audits verify. Actual provider-matched workload and model executables are retained;
new physical stores remain reference-only, not downloaded, hashed or reopened.
[Complete raw/model proof](scale/current-tier3-clock-2026-10-04/worker-clock-92-104/)
extends accepted clock coverage to 65 seeds (27–52 and 66–104), 195,076 invocations,
2,162,000 entries, 1,235 faults and 250,812 history operations. The failed parent,
missing ranges, complete row/matrix and original 24-hour gate remain open.



Worker-clock seed 6 in that same full parent separately fails after all five
child processes exit on a two-second clock-proof timeout. The later parent
stale-sample failure is secondary. Observe each actual clock-worker exit through
the existing fleet error path and preserve the child error before cancellation;
identify publish versus broker-read errors while retaining original deadlines.
Actual-child exit, cleanup, unexpected-success and cancellation controls must
pass normal/race; disabling observation must fail. [Executed observation and
raw failure evidence](scale/worker-clock-checkpoint-2026-10-04/process-exit-observation/)
resolve error masking only; broker cause and real-matrix qualification remain open.
Clock observations must bind broker replies to the exact publication: receipt
stream/nonzero sequence, returned sequence/subject/payload and broker timestamp
must match before replacing the prior sample. Controlled mismatches must leave
that sample unchanged; the compiled original wrong-sequence path must fail.
[Executed normal/race controls and original timeout-phase evidence](scale/worker-clock-reply-identity-2026-10-04/)
prove this fixture correction without changing any deadline or workload gate.
A [three-node real API contract](scale/worker-clock-reply-identity-2026-10-04/real-api/)
then confirms six exact peer publications and existing freshness/offset checks,
normal/race, without treating healthy API conformance as full R5 fault-row evidence.

The old seed-6 timeout was in periodic updates after successful initial writes;
publish versus read and the server cause remain unconfirmed. Runtime/Tier1
producer bytes are unchanged, so existing gates keep their executed-source scope.

Focused ten-minute seed 6 diagnostic 37170797762 at `19f3cb3` subsequently passes
independent review: 3,052 invocations, 33,907 entries, 19 faults and all three
production models over 3,924 operations. All ten checkpoint first attempts pass;
last takes 11.195881565 s under the unchanged bound. The complete canonical
original archive, executable/source provenance and physical stores are preserved
in Git and hashed, not reopened. [Accepted individual diagnostic](scale/worker-clock-checkpoint-2026-10-04/accepted-seed-6/)
does not establish the original startup-timeout cause or qualify the failed
parent, full worker-clock row, full matrix or actual 24-hour gate. Another old-source
full-campaign shard subsequently fails seed 61 with a batch-90 retained-audit
60-second timeout; primary error and complete raw/source proofs are preserved
[without attributing a server cause or relaxing bounds](scale/worker-clock-checkpoint-2026-10-04/failed-shard-53-65/).



Current Tier2 journal seeds 1–200 pass independent raw fault/latency and production
history-model review at exact `c4fed06`: 526,232 invocations, 5,797,467 entries,
3,800 leader kills and worst terminal/progress type p99 17.457496447/9.067125521 s.
The reusable journal-shard reviewer binds the complete requested range and actual
local model dependency inputs to executed Git; it never promotes a parent, full
row, full matrix or soak.
If main has changed, use `--model-root` with a source-isolated campaign checkout;
the entire actual local dependency graph and module hashes must still match
executed Git before/after compilation/review. [Executed source-isolation controls](scale/tier2-model-source-root-2026-10-04/)
prove that selecting a directory cannot bypass a dependency mismatch. [New seeds 49–200](scale/current-tier2-matrix-2026-10-04/)
extend individual journal coverage only. Remaining full 13×200, 16×200 and actual
24-hour full-matrix requirements are unchanged.

The isolation acquisition handoff fixture must publish its arm token atomically:
write and close a staging file, then rename it into place. A direct WriteFile
allows a worker to read the created/truncated marker before token publication
and produce readiness for the wrong token. Deterministically force that
filesystem boundary for initial and replacement tokens; a partial-write error
must preserve the prior published token. Existing acquisition/disarm behavior
must still pass normal/race, and a compiled direct-publication control must fail.
[Executed fixture correction](scale/isolation-arm-publication-2026-10-04/)
closes this local visibility hazard only. The original CI timeout's exact cause,
real fault-row acceptance and full matrix/24h requirements remain separate.


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
contract is accepted. Worker-clock skew was still an R5 port gap at that
preparation revision; the implemented row and its current qualification limits
are recorded below and in the current status snapshot.

The preparation registry had15 rows. The current registry includes worker-clock
and has16 rows. Full campaigns use13-seed shards and340m job budgets:
16xceil(200/13)=256 jobs. Existing14-row/12-seed evidence retains its source and
original scope. No new registry size clears the full original matrix or24h.

### R5 worker-clock diagnostic provenance preparation

The missing R5 worker-clock row must run five separate workers with deliberate
Go wall-clock offsets +5s/-5s/0/+5s/-5s. Preserve the original child JSONL,
compiled overlays/binaries and final process counters. Each initial, periodic
and final clock proof must retain the actual MATRIX_CLOCK GetMsg reply
(subject, sequence, broker time and published payload) beside the worker sample.
Validate all five identities, fresh broker timestamps and requested offsets.
The prepared scripts/tier3-worker-clock-evidence.py helper checks this proof
shape and rejects missing, duplicate, unshifted, stale or fabricated samples.

Diagnostic aggregate copies may subtract only the configured Go overlay from
worker At timestamps, retaining nanoseconds and every other field. Archive raw
aggregates alongside normalized copies and independently compare each copy to
its raw child records. Never adjust controller/broker latency samples or use
network-delay-contaminated measured offsets as timestamp corrections. This
helper and its synthetic controls are preparation; the actual sustained R5
clock row, source/binary provenance and all-row qualification remain required.

### Sixteenth R5 row prepared: separate worker clock skew

`TestFiveContainerMixedWorkerClockSkew` now ports the original worker-clock row
to the sustained R5 six-cell fixture. It starts five actual processes using
three retained race-instrumented executables (+5s/-5s/0/+5s/-5s), keeps NATS and
the controller unshifted, and verifies initial, every30s and final samples
against retained broker messages. Preserve Go time source, exact overlay maps
and patched sources, executable hashes, clean source inventories before/after,
raw child JSONL and original aggregates. Normalize aggregate diagnostic copies
by the fixed configured offset only; independently compare them to originals.
A separate executed Go normalization check is mandatory alongside the row.

The artifact guard requires all five graceful counters, current R5 probe file
replicas, advancing broker sequences and matching fault/probe times. Retain all
physical stores/binaries in member-SHA256-verified archives on success/failure.
The registry now covers16 implemented rows;13-seed shards yield256 jobs for200
seeds, within the hosted limit. This row is prepared, pending actual compilation
and sustained qualification; registry coverage does not prove full-matrix/24h.

The first rolling mixed qualification37107421026 at0bdc838 is rejected: two
upgrades complete, then the third cut's post-upgrade EnsureAuto deployment check
hits its context deadline. Preserve all3795 original archive members, terminal
metadata and logs. This is neither a completed rolling row nor a confirmed
server root cause; investigate the retained evidence before another attempt.

### Rolling deployment-proof budget correction

The rejected first mixed rolling trial uses matrixReadMetadata's2s attempt
context around the entire production EnsureAuto call, which sequentially
checks retained streams, KV buckets and object storage. That helper's budget
is for one metadata lookup. Use the existing whole60s deployment-proof context
for EnsureAuto once; keep individual metadata-read retries and the latency
acceptance gates unchanged. Preserve partial proof phase/error/timestamps on
failure, and record the fallback operation's start/end/deadline/backend/error.
Require completed version2 proofs with successful bounded operation evidence.

A focused actual Go race proof executes two sequential stages spanning2.2s and
preserves cancellation/changed-backend rejection. Its compiled negative restores
the old2s metadata wrapper and must fail the exact named operation-budget test
with context deadline exceeded, not a build/skip/global-timeout failure. Hosted
upgrade-provisioning-budget captures clean committed source hashes before/after
and all JSON events/control bytes. This validates the fixture budget contract;
it does not establish the original NATS cause or clear the mixed rolling row.

### Rolling readiness observations before full provisioning

Require version3 deployment proofs with all5 default public /healthz checks
and retain every round's HTTP status, body, error and before/after timestamps.
The final ordered round must report200/ok with no health errors before whole
fallback provisioning and replica checks, within the same60s proof deadline.
This exposes local stream/consumer recovery beyond one TCP connection or one
selected consumer. Preserve SIGKILL upgrade cuts and their actual scope; the
[documented NATS procedure](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/nats_admin/upgrading_cluster.md)
uses Lame Duck shutdown and healthy endpoints, so this fixture remains a
stronger kill-during-upgrade profile. Do not equate it with normal graceful
upgrade evidence or infer a server defect from temporary recovery warnings.

The original failed third cut occurred after node4 actually restarted on2.15.0,
not before the cut; its third pre-proof passed. Metadata-leader contact returned
about1s after the kill. Retain corrected phase attribution and require actual
post-upgrade recovery evidence before accepting a new rolling trial.

### Version4 rolling semantic admission after asset health

Run default all-peer health before native semantic admission, so recovery
responses are retained even when pinned provisioning later fails. A new peer's
explicit native rejection also performs several stream reads; use the remaining
existing60s proof deadline once for that operation instead of the2s single-read
retry wrapper. Retain each peer's identity, native start/end/deadline, exact
semantic rejection or actual failure even in an incomplete proof. All native
operations must follow the successful health round and precede fallback
provisioning, share its whole-proof deadline and retain strict version/config
rejection. Nil success, transient deadline, cancellation or unrelated errors
cannot substitute for fail-closed native admission.

The focused race contract now exercises both2.2s multi-stage operations and
cancellation/semantic cases. Two separately compiled controls restore the old
metadata wrapper at the fallback and native call sites; each must fail its own
executed named/package operation-budget assertion. This still proves fixture
context handling only; actual mixed row and full-release qualification remain
separate. Preserve both earlier rejected real trials without reinterpretation.


### Non-cancelled timer scheduling retry and restored consumer clocks

The seed55 ahead-clock failure is retained with all five physical pending
snapshots. Its timer stops at StepRequested after a native clock lookup timeout;
a locally successful NAK is followed by no observed fetch before admission
cancellation. The batch stalls and cannot supply another pending timer.
This does not establish server application of the NAK or the original pending
deadline. Preserve the latency and admission gates.

The focused production-worker deterministic diagnostic at223c665 is accepted
under race with two exact replay cases and267 pins. Applied and locally accepted
but unapplied NAK cases take61s/73s virtual time under calibrated stored pending
restoration. This is a diagnostic, not full seeded coverage or a real-row pass.
[Verified raw evidence](scale/timer-error-pending-clock-2026-10-03/).

Next implement and verify bounded recovery for a durable timer request whose
scheduling hint fails while the processing context remains live. The recovery
must preserve the common timer deadline, ownership fencing, durable transfer
before ACK, bounded retry backoff, terminal/journal uniqueness and eventual
physical drain of original and replacement deliveries. A cancelled-heartbeat
handoff alone does not cover this path. Verify the chosen recovery with a
production worker deterministic regression and a precise compiled omission
control, then compare against a real broker case and the original mixed row.
Do not make this diagnostic's61s/73s behavior the new recovery target.


### Repair-backed native timer hints

WithTimerClock requires active compatible domain-aware journal repairers. On a
native backend, SDK ScheduleIsHint now permits a failed scheduling hint after
the timer request commits, while retaining cancellation/domain/support checks.
Worker journaling establishes Suspended before ACK; due repair enqueues the
wake from the canonical deadline. Required fallback publications and legacy
timers retain their previous confirmed-schedule requirement. The hint failure
remains visible in operation records. This avoids a NAK retry whose restored
consumer deadline can delay progress under a shifted stored timestamp.

Verify full current-source seeded/race/replay coverage, a precise compiled
ScheduleIsHint omission, the real three-node injected failure comparison and
new admitted R5 clock evidence before claiming this closes mixed clock recovery.
The original failed seed55 server cause remains unconfirmed; historical
source-qualified diagnostic61s/73s results remain preserved.

### Normal graceful rolling upgrade profile

The existing SIGKILL upgrade profile does not establish the normal graceful
upgrade requirement. `docker-rolling-upgrade-contract` and the sustained
`tier3-mixed-leaders` rolling row now select `shutdown=ldm` and
`upgrade_shutdown=ldm`, respectively. The graceful constructor starts all five
old peers with the documented30s Lame Duck eviction duration and10s grace.
Each upgrade signals SIGUSR2, records a direct original-peer client notification
and ordered old-process shutdown logs, observes stopped state and exact-name
removal, then starts the new binary on the same store. No timeout escalates to
SIGKILL. Per-cut mode/identity/times/logs are retained and checked offline.

Qualify the five-peer retained-message contract at one clean source, including
the existing single-old-peer constructor control and a new compiled SIGKILL
substitution that must fail for missing Lame Duck notification. Then require
the actual ten-minute graceful mixed row, its histories, raw audits, fault
cadence, latency and drain at the same profile. Preparation or the constructor
contract alone does not clear the mixed-row gate,200 seeds,24-hour matrix or
forced two-write-gap requirement. Earlier SIGKILL evidence keeps its own scope.
The documented shutdown procedure is
[NATS Lame Duck mode](https://docs.nats.io/running-a-nats-service/nats_admin/lame_duck_mode).

### Actual Start process crash across a mixed-version upgrade

Extend the R3 mixed-version contract with a real child executing Client.Start
through the production JetStream adapter. Its dispatch boundary stops after
the invocation publish has returned and before WF_RUN reaches the broker.
Require the retained invocation, no matching dispatch/journal and actual
SIGKILL process status. Carry that pending invocation across the old peer's
retained-store upgrade, then require production StartScan repair, terminal
completion and a matching duplicate Start with the original sequence.
Both old-peer-first and auto-fallback-on-new-peer profiles must execute.

`start-upgrade-gap` compiles retained race binaries at clean committed source
and runs the exact omission control: suppress only the gap invocation's scanner
publication. Its absent retained journal must trigger the named semantic failure,
with earlier mixed-version manual-repair tests preserved. Build failures, skips
and global timeouts do not count. This closes a per-phase process-crash evidence
gap when qualified; integrate the same boundary into the sustained R5 rolling
row before claiming full R5 mixed two-write-gap coverage. The200-seed matrix and
24-hour soak requirements remain unchanged.

### Sustained R5 forced Start gap profile

`WF_TIER3_UPGRADE_START_GAP=1` (`upgrade_start_gap` workflow input) reserves one
existing `matrixshort` cohort invocation before each rolling cut. Arm reservation
ten seconds before the scheduled upgrade so the 35-second smoke can supply a
cohort; retain the original scheduled cut time. The child commits through the
selected old peer and is SIGKILLed before dispatch. Only its start-repair
publication is held; other production transports continue unchanged. Carry its
unchanged invocation across the proven retained-store upgrade before releasing
the production fenced scanner. Require acknowledged scanner repair, terminal
completion within30s of kill, and an observed matching duplicate Start.

Keep28 invocations per cohort and all six cell counts, histories, checkpoints,
raw-state integrity, per-type p99 and drain gates. Record the killed SDK call as
uncertain with no response, bounded by actual process death; preserve its raw
commit receipt separately. The artifact verifier requires every announced cut's
process/status/input/sequence/time proof and rejects missing or changed evidence.
The workflow verifier must explicitly require the requested forced-gap and
shutdown profiles; a successful earlier row without these faults cannot satisfy
that request. Include nested crash proofs in regular uploads and preserve their
originals in the complete physical-store archive.

For this forced-gap profile, configure Start scanning at64 entries per100ms
and preserve `start-scan-policy.json` plus `start-scans.json` (actual call start/end,
cursor, result and error). The earlier32/1s policy needs39s to reach sequence1270
from cursor1 even with instant reads; retain that fixed Tier1 counterexample and
its1.9s configured-policy control. This does not identify the failed real run's
cursor or server cause. Require progress evidence in the workflow verifier while
retaining kill+30s, all cell counts, original cut scheduling and every other gate.

Qualification starts with35s race smoke, followed by the full ten-minute profile
for SIGKILL and graceful upgrades. This optional profile is not qualified by
existing R3 or earlier rolling evidence;200 seeds and24h remain required.

### Focused Tier2 consecutive seed replay

`tier2-matrix-leaders` now accepts a positive `start_seed` and preserves actual
seed numbers in job names, fixture environment and artifacts. Default1 retains
the original1..200 release range. Non-1 ranges are focused diagnostic replay;
the existing full-range verifier still requires the complete default range.
Reject non-integer, non-positive and signed64-overflow ranges before launching.
Use this to rerun the retained journal39/86 and consumer90/116/139 failures after
the Start history correction, including their final latency, integrity and drain
gates. Corrected offline histories alone cannot qualify their rejected shards.

### Preserve minimal NATS cleanup originals

The fresh two-message scheduler cleanup reproduction must retain both stores
outside Go's automatic temporary-directory cleanup, plus every copied compiled
upstream source, fixture/control bytes, commands, inventories and Go JSON.
Verify the surviving anchor before the intentional baseline failure. Produce a
complete archive with every member SHA256-readback checked before atomic rename.
Offline review must work after relocation, require explicit retained stores,
and compare harness/fixture bytes against the recorded Git revision. This
strengthens evidence for the isolated missing-source cleanup defect; production
NATS dependency and original million-timer retirement cause remain unchanged.

Use an explicitly selected upstream release candidate to check whether the
same narrow defect is fixed, without replacing the production dependency or
changing the fixture and its original baseline/control assertions. Bind the
selected module to its download version, checksum and Git tag; require complete
before/after `go.mod` and `go.sum` hashes matching the recorded runtime source.
The default remains pinned stable2.15.0. The actual official2.15.1-RC.1 pair at
de2f805 still reproduces the baseline defect and passes the exact dirty-count
control; independently reviewed complete sources/stores are preserved in
[the RC diagnostic](scale/scheduler-upstream-rc1-2026-10-06/). This does not
qualify the original million-timer physical-drain gate or promote the candidate.

### Start scan checkpoints after bounded transient failures

A failed Start scan must not repeatedly restart a confirmed prefix while a large
configured budget exceeds the5s scan-attempt deadline. `ScanResult.RetrySequence`
certifies the first invocation not fully inspected; a journal-read or uncertain
enqueue error keeps that invocation at the retry position. Only a forward,
confirmed prefix may be saved, and only after renewing ownership with a fresh
context. Fatal errors, parent cancellation and lost ownership do not checkpoint.
Lost cursor acknowledgements require reacquisition and rereading the persisted
cursor. Scanners without this certificate retain their existing error behavior.

A fixed128-seed model preserves the old48s/no-progress control and tests cursor
commit/drop/lost-ack plus enqueue acknowledgement uncertainty. A three-node
contract requires actual cursor1→4→7→8 and one retained run message despite a
committed enqueue's hidden acknowledgement. Fencing/fatal/cancellation guards,
all pinned traces and the reconciler package must pass. This fixes partial-timeout
progress; large-population latency and certification for other scan kinds remain
separate work.

Additional full clock-parent failures at executed `79915ca` are independently
retained: seed 112 in 105–117 and seed 121 in 118–130 exhaust the unchanged
60-second batch-90 retained-state audit before secondary cancellation. All five
worker logs pass in each case; seed 112's affected workflow has a completed
client result 203.034541651 s before audit start. This is a lookup-timeout boundary,
not confirmed missing/corrupt data or a server-cause attribution. Complete raw
artifacts and exact pre/post source hashes are preserved; new stores/executables
are reference-only. [Failure proof and qualification limits](scale/worker-clock-checkpoint-2026-10-04/failed-shards-105-130/)
keep those ranges, full parent/row/matrix and original 24-hour gate open.

The failed clock seeds 112/121 now have separate current-source instrumented
10-minute diagnostics at exact `3b2999e` (runs 37183948633 / 37183950090), using
existing per-attempt checkpoint timing and primary-error preservation with
unchanged three 20-second attempts / 60-second total audit limits. Both diagnostics are independently accepted: all ten checkpoints per seed pass
on their first attempt, with maximum durations 18.675611114 / 19.351887878 s.
Actual clock/model executables, all three models over 7,560 history operations,
source inputs and complete raw evidence are retained and SHA-verified. Both full
original-store archives and their members verify, but stores were not reopened.
These runs did not reproduce the historical failures; their cause and failed
full-parent qualification remain open. [Complete diagnostic proof](scale/worker-clock-checkpoint-2026-10-04/seeds112-121-diagnostics/accepted/). [Exact diagnostic scope and runtime graph](scale/worker-clock-checkpoint-2026-10-04/seeds112-121-diagnostics/)
preserve those boundaries while full campaigns continue.

The complete Tier2 journal-leader row at exact `c4fed06` now independently
qualifies all 200 seeds across 17 terminal successful shards: 526,232 invocations,
5,797,467 entries, 3,800 recorded leader kills and all three production models
for 676,814 operations. Original archive/Git parts/member hashes, unique complete
range coverage, source/job identities and common 45 model dependency bytes verify.
Earlier 1–48 raw histories are restored and rechecked to close their narrower
recorded dependency comparison, without repeating runtime trials. Worst terminal/
progress p99 is 17.457496447/9.067125521 s under unchanged 30 s / 10 s gates.
[Complete executed-source row proof](scale/current-tier2-matrix-2026-10-04/journal-full200-qualification/)
qualifies this row at c4fed06, not the current-source complete matrix, other twelve
rows, the original physical million-timer drain or actual 24-hour full-matrix soak.
Actual workload binaries/physical stores were not uploaded; final integrity/drain
assertions retain named-test scope, with no independent store reopening claimed.


Tier2 consumer-leader seeds 13–24 at executed `c4fed06` are now independently
qualified from complete raw faults/latencies and all three production history
models: 31,080 invocations, 342,529 entries, 228 kills and 39,986 history
operations, with all 45 model dependency inputs matching source. Actual model
executable and complete proof are retained. Worst terminal/progress type p99 is
18.076655685 / 7.320884037 s under unchanged 30 s / 10 s gates. The shard reviewer
now supports explicit consumer selection while retaining journal defaults and
rejecting row substitutions. [Complete consumer shard proof](scale/current-tier2-matrix-2026-10-04/consumer-13-24/)
qualifies only this twelve-seed range; full consumer/current-source matrices,
physical million-timer drain and the actual 24-hour soak remain open.


The consumer-leader row at executed `c4fed06` now independently qualifies seeds
1–24: newly reviewed 1–12 adds 30,912 invocations, 340,629 entries, 228 kills
and 39,786 source-bound history operations. Raw faults/latencies and all three
models pass, with actual model executable and complete hash-verified proof retained.
Worst new terminal/progress type p99 is 17.358545411 / 7.069948845 s under
unchanged 30 s / 10 s gates. [Complete 1–12 proof](scale/current-tier2-matrix-2026-10-04/consumer-1-12/)
plus accepted13–24 establishes 61,992 invocations / 683,158 entries / 456 kills;
full consumer/current-source matrices and original actual24h gate remain open.


The consumer-leader row at executed `c4fed06` now independently qualifies seeds
1–36. New25–36 adds 31,108 invocations, 342,890 entries, 228 kills and 40,040
history operations passing all three models with exact45 actual dependency checks.
Worst new terminal/progress type p99 is 16.754409553 / 7.335288524 s under
unchanged 30 s / 10 s gates. [Complete 25–36 proof](scale/current-tier2-matrix-2026-10-04/consumer-25-36/)
retains actual model executable and complete hash-verified raw/source/API/reviewer
bindings. Accepted1–36 totals93,100 invocations / 1,026,048 entries / 684kills /
119,812 history operations. Full consumer200/current-source matrices and original
actual24h gate remain open. [Post-restart evidence availability](scale/post-restart-evidence-2026-10-04/)
records the50 GiB root expansion, lost historical RAM paths and verified durable
disk restoration of both clock-diagnostic physical originals without changing
qualification or attributing the original audit failures.


Tier3 worker-clock seeds131–143 at executed799 are independently qualified:
41,076 invocations / 455,262 entries / 247 faults, with all three production
history models passing52,812 operations and all45 actual dependencies matching
source. All759 captured pre/post inputs per seed,273 clock proofs,1,365 broker
messages and143 cohort audits verify. Worst terminal/progress type p99 is
5.075736715 / 0.282275176 s under unchanged30 s /10 s gates. Every51,519 own
canonical original member verifies; actual workload/model executables and complete
compact proof are retained in Git, while physical originals remain on root disk/
GitHub without reopening. [Complete131–143 proof](scale/current-tier3-clock-2026-10-04/worker-clock-131-143/)
brings accepted same-source coverage to78 seeds /236,152 invocations /2,617,262
entries /1,482 faults /303,624 history operations. The original failed ranges/
parent, full200-seed row/current-source matrices and actual24h gate remain open.


Tier3 worker-clock144–156 at executed799 independently qualifies41,272 invocations,
457,464 entries,247 faults and53,064 operations passing all three production
models. All759 captured selected inputs per seed/45 actual model dependencies,
273 clock proofs/1,365 messages/143 cohort audits verify; worst terminal/progress
p99 is5.076522145 /0.284237615s under unchanged30s /10s gates. All original raw
uploads and actual model binary are retained. Three byte-identical workload
executables share full-revision/member/hash-bound references to accepted131–143,
with a verified39-path restore command; duplicate payloads are omitted. Own new
physical-store artifact is reference-only. [Complete144–156 proof](scale/current-tier3-clock-2026-10-04/worker-clock-144-156/)
brings accepted same-source clock coverage to91 seeds /277,424 invocations /
3,074,726 entries /1,729 faults /356,688 history operations, without repairing
failed historical ranges/parent or qualifying full current-source matrices/24h.


Clock157–169 at executed799 now independently qualifies39,368 invocations /
436,869 entries /247 faults, with all three models passing50,616 operations.
Source759/model45 inputs,273 clock proofs/1,365 messages/131 cohort audits verify;
worst terminal/progress p99 is5.251218399 /0.592107855s under unchanged30s /10s
limits. [Complete proof](scale/current-tier3-clock-2026-10-04/worker-clock-157-169/)
retains original raw/model bytes and verified pinned SDK restoration. Own new
physical stores remain reference-only. Accepted same-source clock coverage is104
seeds /316,792 invocations /3,511,595 entries /1,976 faults /407,304 history ops;
failed parent/current-source matrices/original24h stay open.

The unresolved continuation/promise after-manifest boundary now has optional
retained original stores and exact pre-kill read timing/error classification.
[Prepared diagnostic](scale/continuation-promise-retained-diagnostic-2026-10-04/)
compiles with the disabled profile; no native cut, runtime fix, timeout relaxation
or original server-cause resolution is claimed. Captured actual-executable/source/
store diagnostics and complete eight-cut qualification remain required.


### Repeated worker faults with busy partition reassignment

The focused combined Phase3 case now qualifies at executed `e9ad9ab`:200 workflows
×50 steps on six workers complete in192.806390s under the original five-minute
target, with two-second worker faults, five-second assignment scheduling, actual
45-second holds and a confirmed server minority route cut. Independent raw
journal checks and200 SDK replays pass. The asynchronous assignment controller
restores available destinations; held workers remain under faults but are excluded
from new assignment placement. [Complete proof](scale/phase3-repeated-rebalance-2026-10-04/accepted/)
retains actual SDK/source/replay/store bytes. This qualifies this combined case
only; remaining Phase3 requirements, full matrices and actual24h stay open.


### Complete Tier2 consumer-leader row accepted

All200 consumer-leader seeds at executedc4fed06 are independently accepted across
17 complete600-second shards:519960 invocations /5730955 entries /3800 kills,
all three models for669084 operations. Complete archive/member hashes, unique
seed coverage, every retained actual model and45 Git-matched dependencies verify.
Worst terminal/progress type p99:18.076655685/9.110842432s under unchanged R3
30/10 gates. [Complete row proof and aggregation](scale/current-tier2-matrix-2026-10-04/consumer-full200-qualification/)
qualifies this executed-source row, alongside journal200, not the final-source
full13-row matrix, Tier3 matrix, original million-timer physical drain or actual
24-hour full-matrix soak. Native SDK/stores were not uploaded; final integrity/
drain assertions retain named-test scope, without independent store reopening.


### Tier2 all-server-kill shards accepted

Two completed shards independently qualify seeds1–24 atc4fed06:56056 invocations,
617750 entries,456 events recording every node killed before restart, and all
three models for72130 operations with45 Git-matched dependencies. Original ZIP,
raw evidence, actual model executables and complete SHA-verified proof archives
are retained. Worst terminal/progress p99:18.295491603/12.629939729s under the
original distributed-verification30s fault gates, measured from enabling events.
Journal/consumer progress below10s also satisfies a stronger observed bound;
the original full-matrix fault progress requirement is30s. Native SDK/stores
were not uploaded; final integrity/drain assertions retain named-test scope.
[Seeds1–12](scale/current-tier2-matrix-2026-10-04/cluster-1-12/) and
[Seeds13–24](scale/current-tier2-matrix-2026-10-04/cluster-13-24/)
qualify those ranges only; complete cluster200/current-source matrices and
actual24h remain open. Five reviewer identity/substitution guards pass.


### Reproducible scheduler server candidate and native diagnostic

A builder copies pinnedNATS2.15 outside the checkout, applies the reviewed
scheduler dirty-count fix onlytofilestore.go, captures1435 Go/module compiler
inputs and retains actual binary/build info. All598 upstream inventories,
selected pre/post bytes and actual process identities verify. Module cache and
production dependency remain unchanged. The timer-volume command can explicitly
retain/use this binary across both all-node SIGKILL restarts; offline verification
checks its digest and rejects candidate release promotion even at million scale.
Command tests pass2.998s. Actual300 timers over90s finish both full restarts and
all300 receipts with zero messages/pending on all physical replicas, but raw
p99=10.482509603s fails unchanged2s gate. Failure and all stores/executables/sources
are preserved, not independently reopened; no campaign pass or rerun. Both
outages fall in the compressed population; original million/24h requirement stays
open. [Complete preparation and failed-native proof](scale/scheduler-server-candidate-2026-10-04/)
records the source, first build/VCS failure and corrected build identities.


### Batched actual24h audit capacity failure and larger bounded delivery window

The actual b287e98 batched24h journal attempt terminal fails after3223.15s at
batch400/cutoff11200, after accepting cohort10920. Three20s attempts reach the
original60s cap. Two complete bulk scans without fetch errors then expire in
validation; middle fetch/fallback reads time out. No absent-state/corruption or
NATS-cause claim. All5271 original members,1222 Git inputs and actual race SDK/
build info verify; complete99209919-byte proof is retained in four hashed parts,
with original stores not reopened. [Complete failure](scale/local-r5-soak-24h-2026-10-04/journal-batched-audit-failure/).

The opt-in reader now prepares bounded4096-record pulls to reduce repeated
request overhead; capture bounds, gap/tail proof, full invariant body, no-cache
behavior and20s/60s budgets remain unchanged. New12k invocation/144k entry native
case compares previous512 and new4096 windows over identical retained state.
Actual outcomes and large-population/fault capacity remain pending; no soak rerun.


### Larger-window comparison completed without material speedup

Retained race SDK ate638a93 completes full12000/144000/12000 audits of identical
stores:512 window14.853547828s,4096 window14.851015237s; point reaches original20s
deadline. Fresh corruption/cohort/compaction controls pass. All2889 captured inputs,
59 Git-local files, actual live SDK/build info and3984 complete proof members
verify, stores not reopened. [Complete comparison](scale/retained-audit-batch4096-2026-10-04/).

No material gain is demonstrated. Default opt-in batch size is restored to512;
explicit native512/4096 comparison remains. No soak restart or deadline change.
Per-record delivery/validation and efficient fault recovery need further work
before the original24h gate can qualify. All original full-matrix/million gates
remain open.


### Consumer replication cost comparison and phase profiling

Actual race SDK atddd5396 audits identical12k/144k stores withR3, R1 thenR3
temporary consumers:12.024088088 /12.653120485 /14.682479197s, all complete.
Source streams remainR3; actualR1 configs and zero consumer leakage verify.
All2889 selected inputs /59 Git-local files, actual live SDK/build info and3270
complete original proof members verify, stores not reopened. No clear gain is
demonstrated. [Complete comparison](scale/retained-audit-consumer-replication-2026-10-04/).

Production replicas/batch size and original20s/60s budgets stay unchanged. R1
consumer fault recovery and full release gates remain open. A test-only native
CPU profile and stream-scan/visitor timing diagnostic is prepared to locate
per-record cost; it includes embedded server activity and does not establish
five-container performance or a server-cause claim. No soak restart.

### Native audit instrumentation and population scaling

Actual source1c9fb4d completes equivalent fresh12k/144k full audits in
14.672637649s with race instrumentation and1.669375955s normally. Both exact
reports agree. Independent source/SDK/profile/complete-archive reviews verify;
live/proc identity was not captured and stores were not reopened. Embedded
server/client CPU profiling shows substantial TSAN overhead; this does not
establish five-container capacity or justify promoting earlier deadline failures.
[Full profiles](scale/retained-audit-phase-profile-2026-10-04/).

The native test now accepts explicit WF_AUDIT_BATCH_PROFILE_INVOCATIONS from1
through100000, retaining default12000 and exact12-record-per-invocation/report
assertions. Next measure a full100k normal cohort under the original20s audit
limit. This is a scaling diagnostic, not full workload/fault/24h qualification.
Production code, race gates, audit budgets and all release requirements stay.

The100k normal diagnostic at6a38a7c fails original20s at20.092516379s after
both exact stream populations are read (INV1.924s/JRN17.208s), before full
per-journal/terminal validation. GC scanning/assistance is substantial in the
embedded-server/client CPU profile under512MiB. All original evidence and live
SDK/input/archive identities are independently verified and preserved. Normal
instrumentation alone is insufficient; measure memory sensitivity next while
retaining this failure, then address complete-audit memory/scaling. No budget,
production setting or release requirement changes.
[Complete failed100k proof](scale/retained-audit-phase-profile-2026-10-04/normal-100k/).

### Explicit audit memory profile and five-container comparison

Actual source4b7531e uses the byte-identical100k normal SDK with2GiB:
full100000 journals /1.2M entries /100000 terminals complete in15.487657247s
under the original20s deadline. All original sources, live SDK, CPU profile,
native stores and full five-part archive verify; stores are not reopened.
This demonstrates embedded-fixture memory sensitivity, not race/fault/full-matrix
or24h qualification. The original512MiB failure remains failed.
[Complete comparison](scale/retained-audit-phase-profile-2026-10-04/normal-100k-2g/).

The real five-container runner now accepts --memory-limit (512MiB default,
1GiB,2GiB,4GiB), validates before starting, and captures selection in execution
and test-environment evidence. Inherited budgets cannot silently override it.
Fault schedules, checkpoint audits, budgets, SyncInterval, default race and
all semantic release gates stay unchanged. Five producer controls pass. Next
bounded10m journal diagnostic uses explicit2GiB/normal plus full audit traces;
review its actual original evidence before longer campaigns. Complete-audit
memory/scaling, large-population faults and original24h/full matrices remain.

### All-server-kill qualification extended through48

Complete600s cluster25–36 and37–48 shards atc4fed06 are independently accepted:
57232 invocations /630935 entries /456 all-three-node fault events /73649
exactOk history-model operations. Raw fault identities/cadence/latency clocks
and five-minute completion deadline verify;45 actual model inputs match Git;
actual models and complete original proofs retained with member/part readback.
Totalcluster1–48 now113288 invocations /1248685 entries /912 events; worst
terminal/progress per-type p99 remains18.295491603/13.014859048s under original30s.
Native SDK/stores not uploaded, so integrity/drain stays named-test scope.
Full200/final-source/full matrices and actual24h remain open.
[25–36](scale/current-tier2-matrix-2026-10-04/cluster-25-36/) ·
[37–48](scale/current-tier2-matrix-2026-10-04/cluster-37-48/).

A bounded normal2GiB journal/seed1/10m real five-container diagnostic is now
live at4a875f0; actual SDK/environment and persistent supervisor observed.
Original20s/60s audits,512 batches and30s liveness unchanged. No terminal/full
original/fault-row/24h qualification follows from launch.
[Live snapshot](scale/local-r5-audit-memory-2026-10-04/normal-2g-ten-minute-launch/).

### Fresh KV snapshot candidate for complete audits

The100k profile leaves about4.866s outside stream scans, including100000
terminal Get calls. A separate opt-in state reader now obtains a new documented
WatchAll latest-value set for each audit, accepts only the initial nil completion
marker, stops/drains the watcher, preserves delete/purge absence, and limits
cohort retention to eligible invocation/snapshot keys. Closed partial watches,
invalid entry identities/revisions/operations, cancellation and out-of-order
revisions fail closed. Each complete watch attempt uses the existing bounded
read/retry helper. No values or results persist between audits.

The same checker's body and checkJournalRecords remain; CheckSnapshot bytes
and existing default point/batched readers are unchanged. Native comparison
controls now compare the point, batched and fresh-state readers on compaction,
cohort exclusion, fresh corruption and I1/I2/I3/orphan cases, plus terminal
replacement/Delete/Purge/recreation. A same-store native100k comparison uses
point-state before, snapshot-state, point-state after under original20s limits.
Unit/race integrity passes; native equivalence/performance/fault qualification
is still pending. Production harness has not adopted this reader.

The normal2GiB real five-container ten-minute journal diagnostic at4a875f0
passes its named test and producer row checks:2436 invocations /26869 entries /
19 faults. SDK/supervisor terminal and complete producer original archive made;
independent complete-original review and publication remain pending. This is
recorded normal-profile evidence, not race/full-matrix/24h qualification.

### Fresh state reader equivalence and normal-profile originals reviewed

At9b2a182 the race native controls prove point/batched/state-snapshot exact
reports and errors through compaction/cohort/fresh corruption/I1/I2/I3/orphan
and terminal replacement/Delete/Purge/recreation. Same-store normal2GiB100k /
1.2M reads agree:point-state15.043925816s, snapshot14.468247646s, point recheck
19.93021917s under original20s. Variability prevents reliable speedup claim;
no production-harness adoption. All selected/Git/actual SDK/build-info/archive
member/part identities verify;100k live/proc captured, controls not; stores not
reopened. Snapshot checker bytes match283ba32. Candidate fault/legacy/scaling
qualification and full release requirements remain.
[Native complete proofs](scale/retained-audit-state-snapshot-2026-10-04/).

The normal2GiB real five-container journal10m at4a875f0 now has independently
reviewed complete originals:2436 invocations /26869 entries /19 faults, eight
mandatory audits max1.851s under original20s/60s, raw30s liveness checks and
three exactOk models for3132 operations. Actual SDK/stores,1237 local source
files,45 model source inputs and actual model retained; complete original and
review/model archives read back/hash verify. Stores not independently reopened.
SDK/supervisor terminal; no containers remain. This is normal-profile diagnostic
qualification, not race/full200/current-source/full-matrix/24h acceptance. Older
24h failures remain failed; no new longer run follows.
[Reviewed complete originals](scale/local-r5-audit-memory-2026-10-04/normal-2g-ten-minute/).

### Streaming retained journal audit candidate

The next opt-in audit mode decodes every eligible retained record freshly but
keeps per-invocation protocol state rather than all decoded prefixes. It tracks
index/sequence/epoch/owner, pending/completed request payloads, signals/attempts
and terminal bytes. Monotonic epochs permit retaining only the current epoch's
owner. Terminal records release request/completion payloads. Memory remains
proportional to invocation count and current payloads, not complete entry count.
No journal/state result is reused between audits.

Errors are saved until the existing sorted subject reduction, preserving
raw-decode-before-invariant precedence and orphan/key validation. Compacted
subjects ignore the raw-prefix summary and use the original logical journal
reconstruction and slice-based checker. Original checkJournalRecords and
modeled CheckSnapshot bytes remain unchanged as differential oracles; default
readers and production harness are not switched.

Race unit tests compare1000 seeded valid histories, every prefix, terminal
lookup errors/mismatches, twelve mutation classes, arbitrary mixed histories,
blank owners/epoch changes and committed continuation matching. A200001-entry
prefix with100000 epochs performs no per-entry/epoch allocations. Native
compaction/cohort/corruption/terminal controls now also compare streaming modes
with point/batched/state readers; native equivalence still pending. A same-store
100k benchmark compares old, streaming, streaming-plus-state and old recheck
under unchanged20s. Whole embedded-process cumulative allocations/GC cycles
are recorded, not pure checker cost or peak RSS. No performance, fault-recovery
or release qualification is inferred from unit tests or preparation.

Native qualification at1d43a2b now verifies point/batched/state/streaming exact
reports/errors through compaction/cohort/fresh corruption/I1/I2/I3/orphan and
terminal replacement/Delete/Purge/recreation. Same-store100k/1.2M normal2GiB
four complete audits under20s: baseline17.401s, streaming-point17.300s,
streaming-state11.810s, baseline recheck18.983s. Combined mode faster in this
fixture, streaming alone close to baseline; allocation/GC counters include
embedded servers and client, not peak/checker-only memory. Original oracles
unchanged; all selected/Git/actual SDK/build-info/member/part proofs verify.
100k live/proc/env captured, controls not; stores not reopened. No adoption or
soak restart. Next full-cohort leader-loss/cancellation/state-watch/legacy faults,
then real five-container qualification under unchanged20s/60s.
[Complete native proofs](scale/retained-audit-streaming-2026-10-04/).

### Full streaming audit journal-fault controls

New opt-in native controls publish500 invocations /2000 journal entries /
500 terminal values, establish an original full-audit baseline, then run each
streaming mode with point or fresh-state reads. During WF_JRN delivery128,
observe the actual named memory/AckNone/R3 consumer, require nonzero pending,
identify its consumer leader and shut down that real in-process NATS node.
Require all2000 records and the same500/2000/500 full report under original20s,
with no remaining invocation/journal audit consumers. Client reconnects across
all three fixture URLs. This is library shutdown, not OS SIGKILL.

Paired cancellation at the same delivery boundary must stop callbacks at128,
return context.Canceled and the same invocation-only partial report; no partial
journal result can certify completion. Actual retained race execution at4616d47
passes all four cases; recovery0.557s/0.445s and cancellation boundaries are
independently reviewed with complete originals.
[Complete controls](scale/retained-audit-streaming-2026-10-04/journal-faults/).

A larger race control at71e6ad7 first establishes a full combined baseline for
12000 invocations/144000 entries/12000 terminals. It shuts down the actual R3
consumer leader atvisitor128 with143488 pending and restores the exact full
report in10.745s under original20s. Consumer cleanup, captured live executable,
all selected/Git inputs and complete originals verify.
[Complete large-tail control](scale/retained-audit-streaming-2026-10-04/large-journal-fault-race/).
These controls do not test interruption during state-watch delivery, OS SIGKILL,
legacy or R5 adoption;100k fault capacity and full release gates remain open.


The normal-build 100k fault capacity control at878c341 now passes the same
complete baseline and actual consumer-leader shutdown with1199488 pending.
All1.2M entries and100000 terminal results validate in11.675s under original20s;
consumer cleanup and actual live SDK/environment/source inputs verify.
[Complete 100k fault proof](scale/retained-audit-streaming-2026-10-04/large-journal-fault-100k/).
This is normal2GiB/GOMAXPROCS2 native capacity, not race100k/five-container/
state-watch interruption/OS SIGKILL/legacy or actual24h qualification.


### Full streaming audit state-watch fault controls

Retained race controls at722305a now interrupt real WatchAll deliveries inside
full12000-invocation/144000-entry audits, after a mandatory full baseline. At
initial delivery128, identify the actual pending watch consumer and shut down
its leader. The SDK-created consumer has one replica, with11125 pending at the
kill. All12000 journals/144000 entries/12000 terminals validate in12.807s under
the original20s deadline. Paired cancellation stops at128 and returns only the
invocation count with context.Canceled in11.394s. No test-generated values or
completion marker enter the checker. All selected/Git inputs, live SDK/env,
full build-info and complete original archive verify.
[Complete state-watch controls](scale/retained-audit-streaming-2026-10-04/state-watch-faults/).
This closes the native12k watch-interruption control, not OS SIGKILL, legacy,
five-container/full-matrix or actual24h qualification. Default readers stay
unchanged; next expose the combined candidate explicitly in the real harness.


### Explicit combined reader in the real cluster harness

`CheckWithStreamingStateReads` and its captured-cohort counterpart expose the
already controlled combined algorithm. `run-tier3-soak.py` now accepts
`--streaming-state-retained-audit` and records the choice. The checkpoint and
final whole-state audit select the same reader. Conflicting batch/combined modes
fail closed; default readers,20s/60s audit deadlines,30s fault gates and2m server
sync remain unchanged. Producer controls ensure environment flags cannot
silently activate the candidate. Next run the actual five-container journal
row for ten minutes with race and explicit2GiB memory, then independently review
all originals before making any longer-run qualification claim.


### Combined audit legacy compatibility and first real race outcome

Full legacy compatibility atb3c64f1 passes real three-process NATS2.11.17,
compaction/cohort/fresh corruption/snapshot/orphan controls and both public
combined APIs. Exact errors match the original oracle. First wrong-error-string
run is preserved as failed. [Complete legacy proof](scale/retained-audit-streaming-2026-10-04/legacy-full/).

The actual five-container race2GiB ten-minute journal row atd7e075d fails at346.89s:
node3 is restarted after the ninth SIGKILL, but WF_JRN does not become current
within the original60s heal deadline (reported lag9058). Four checkpoint audits
pass, max2.659s; the final row is not qualified. Complete originals, source,
live SDK/build-info/environment and raw failure diagnostics independently verify.
Node3 route snapshot has onlynode0 as peer (four pooled routes); cause unconfirmed.
Next diagnose route discovery/rejoin and replication before an unchanged rerun
or longer soak. [Complete failed original](scale/local-r5-streaming-audit-2026-10-04/race-2g-ten-minute-failed/).


### Explicit route-seed diagnostic comparison

The failed original atd7e075d observed restartednode3 with onlynode0 as route
peer, despite four pooled connections. Existing fixture bootstrap seeds one
peer and relies on discovery. Add explicit `--explicit-route-seeds` to seed all
other route-only aliases on every container restart, while recording the choice
and clearing inherited activation flags. Defaults remain single-peer discovery.
A per-fault census validates the monitoring identity/count and groups pooled
connections by actual peer names/IDs, recording missing members. Four pooled
routes to one peer must not be mistaken for a four-peer mesh.

Run the same journal/seed1/10m/race2GiB profile with explicit seeds and retained
census after every fault. Keep original cadence,20s/60s audit deadlines,60s heal
and30s liveness gates. This is a configuration comparison; a pass alone does
not establish the cause of the original failure or qualify default/full-matrix/
actual24h release. Review complete originals and topology observations first.


### Explicit-route comparison accepted at its executed source

The five-container race2GiB/combined-reader journal row at0393caa passes the
original ten-minute workload, 19 faults, all eight audits and final named checks:
2324 invocations/25596 entries; worst terminal/progress p99 12.029s/6.628s,
max checkpoint3.959s. All19 post-fault censuses show all four peers per node.
Complete originals, raw row, three independently rebuilt exactOk models, live
SDK/env/build-info and all1266 source inputs verify. Stores are retained, not
reopened. [Complete reviewed comparison](scale/local-r5-streaming-audit-2026-10-04/explicit-routes-race-2g-ten-minute/).
This qualifies this explicit-route row, not the cause of the previous failure,
the default profile, complete matrices or actual24h. Longer qualification remains.

### Recorded-source all-server matrix coverage through seed72

Two more independently reviewed600s shards atc4fed06 qualify all-server-kill
seeds49–72, extending accepted coverage to1–72:170520 invocations/1879560 entries/
1368 faults/219419 model operations. Worst terminal/progress p99 remain18.295s/
13.015s under original30s gates. Complete original/member/model/part proofs read
back. Native SDK/stores not uploaded; named final integrity/drain scope only.
[49–60](scale/current-tier2-matrix-2026-10-04/cluster-49-60/) ·
[61–72](scale/current-tier2-matrix-2026-10-04/cluster-61-72/).
Successful73–96 await review; full200/current-source/full matrices and24h remain.

### New actual24h explicit-route normal-profile launch

The journal/seed1 24-hour run started at executed `95b63c0` on 2026-10-04
21:08:58 UTC. Five containers, normal build, explicit 2 GiB/GOMAXPROCS2, combined
streaming/state retained audits and all-peer route seeds are recorded in the
[verified launch](scale/local-r5-streaming-audit-2026-10-04/explicit-routes-normal-2g-24h-launch/).
Original audit/liveness/heal/sync gates remain. This is observed launch evidence;
terminal originals, model review and actual24h acceptance remain pending.

### Recorded-source all-server matrix coverage through seed96

Independent review now accepts exact `c4fed06` all-server-kill seeds1–96.
New seeds73–96 retain complete originals, raw fault/latency checks and three
rebuilt exactOk history models; model dependencies match executed Git. Aggregate
227668 invocations /2509494 entries /1824 faults /292953 operations pass original
30s gates (worst terminal/progress p99 18.295s/13.015s). Full200/current-source/full
matrices remain open; native SDK/stores were not uploaded and final integrity/
drain remains named-test scope.
[73–84 proof](scale/current-tier2-matrix-2026-10-04/cluster-73-84/) ·
[85–96 proof](scale/current-tier2-matrix-2026-10-04/cluster-85-96/).

### Recorded-source all-server matrix coverage through seed108

Exact `c4fed06` all-server-kill coverage now accepts seeds1–108 after raw checks
and three rebuilt models over37283 new operations for97–108. Original30s gates
remain; aggregate256648 invocations /2828891 entries /2052 faults /330236 operations
has worst terminal/progress p99 18.295s/13.015s. Complete originals and all archive
members/parts verify. Native SDK/stores unavailable; final integrity/drain remains
named-test scope. Full200/final-source/full matrices remain open.
[Complete new shard](scale/current-tier2-matrix-2026-10-04/cluster-97-108/).

### Persist validated route peer identities

Future explicit-route diagnostics save each validated remote server ID beside
peer-name pool counts. Race controls verify serialized identities for single-peer
pools and full meshes and reject identity aliasing. Historical census outputs
retain their original scope; the live95b63c0 soak is unchanged. Saved topology
identities help independent review but do not prove quorum/catch-up or causes.

### Recorded-source all-server matrix coverage through seed120

Exact `c4fed06` coverage now accepts all-server-kill seeds1–120. New109–120
passes raw checks and three rebuilt models over37206 operations, original30s
gates retained. Aggregate285572 invocations /3147706 entries /2280 faults /367442
operations has worst terminal/progress p99 18.295s/13.015s. Complete original archive
and all members/parts verify. Native SDK/stores unavailable; final integrity/drain
remains named-test scope. Full200/final-source/full matrices remain open.
[Complete new shard](scale/current-tier2-matrix-2026-10-04/cluster-109-120/).

### Combined continuation retirement and state-leader restart control

The retirement/GC/generation-reuse fixture now combines fresh manifest response
loss with actual confirmed state-stream leader library shutdown/restart. Local
race control passes22.05s; both original baselines pass43.40s. Raw integrity,
old-generation rejection, peer-visible results and shared reference preservation
remain strict. [Overlay and logs](scale/continuation-retirement-state-leader-2026-10-04/)
record exact local scope; SDK/stores were not retained. OS SIGKILL, active-writer
GC, lease/TTL/limit combinations and final-source matrices/24h remain open.

### Combined retirement/reuse with actual server SIGKILL

Focused race cuts pass for fresh-manifest response loss plus confirmed state
leader SIGKILL/restart and all-three-server SIGKILL/restart. Test67.74s; original
library control25.74s. Actual exits/replacements, raw integrity, generation
rejection and shared object preservation remain checked. All855 retained members
verify, including actual SDK/process stores and selected source ledgers.
[Complete originals and scope](scale/continuation-retirement-process-2026-10-04/corrected/).
First startup failures are separately preserved; readiness now retries4s attempts
within the same30s budget. Worker SIGKILL/active-GC/lease/TTL/limit/p99/final-source
full-matrix/24h combinations remain open. No stores were independently reopened.

### Combined retirement/reuse with actual lease expiry across server SIGKILL

Focused race cut passes57.78s after confirmed all-three-server SIGKILL,13.0003s
outage against unchanged12s TTL, manifest response loss and generation reuse.
Terminal epoch78 exceeds captured54; three effects/two terminals and shared
references remain exact. Complete438-member SDK/process-store proof verifies.
[Originals and scope](scale/continuation-retirement-lease-expiry-2026-10-04/).
Budgets unchanged; no physical reopening or worker SIGKILL/active-GC/other TTL/
limit/p99/final-source full-matrix/24h qualification is claimed.

### Recorded-source all-server matrix coverage through seed132

Seeds121–132 at `c4fed06` independently qualify28,392 invocations/312,903 entries,
228 faults and36,518 exactOk history operations under unchanged30s gates.
All45 actual model dependencies match Git; complete archive members and parts
read back. Combined1–132:313,964 invocations/3,460,609 entries/2,508 faults/
403,960 operations. Worst terminal/progress p99 remains18.295s/13.015s.
SDK/stores unavailable; named-test integrity/drain scope. Full200/final-source/
13×200 gate remains open. [Complete shard proof](scale/current-tier2-matrix-2026-10-04/cluster-121-132/).

### Complete recorded-source ahead-clock200 row accepted

All200 complete600-second seeds at `63fbc03` independently qualify128,996
invocations/1,419,968 entries/3,800 admitted faults/167,272 exactOk history
operations. All-five common clock and pending timer admission, checkpoint/raw
report/explanation/fencing regeneration,704 clean source inputs and45 actual
model dependencies verify. Worst terminal/progress p99 is18.793s/28.101s under
unchanged30s gates. Complete downloaded raw evidence/model/reviewer originals
and parts read back. Physical-store metadata remains references only; actual
workload SDK/stores not downloaded or reopened, integrity/drain named-test scope.
This clears the complete recorded-source ahead-clock row only; final-source/
full16×200/behind-clock/million-drain/24h gates remain open.
[Complete ahead-clock raw proof](scale/r5-ahead200-2026-10-04/complete-recorded-source-raw/).

### Recorded-source all-server matrix coverage through seed144

Seeds133–144 at `c4fed06` independently qualify28,728 invocations/316,699 entries,
228 faults and36,950 exactOk history operations under unchanged30s gates. All45
actual model dependencies match Git; complete archive members/parts read back.
Combined1–144:342,692 invocations/3,777,308 entries/2,736 faults/440,910 operations;
worst terminal/progress p99 remains18.295s/13.015s. SDK/stores unavailable,
integrity/drain named-test scope. Full200/final-source/13×200 remains open.
[Complete shard proof](scale/current-tier2-matrix-2026-10-04/cluster-133-144/).

### Recorded-source real disk-delay seeds1–13 accepted

Complete13×600s shard at79915ca independently qualifies41,188 invocations,
453,656 entries,247 actual100ms dm-delay faults and52,956 exactOk history
operations. Writable node4 mounts, admitted R5 journal/dispatch leaders,
five-second intervals, delayed sync and same-device restoration verify per cut.
All raw result/explanation/fencing reports and143 checkpoint audits pass;
terminal/progress p99=5.488s/0.642s under original30s gates. All45 actual model
dependencies match Git; complete1394-member archive/two parts read back.
Workload source attribution is checkout/header binding, without captured full
source inventory or actual SDK/native stores. Integrity/drain named-test scope.
Full200/final-source/full16×200/24h remain open.
[Complete disk-delay proof](scale/current-tier3-block-delay-2026-10-04/seeds-1-13/).

### Recorded-source real disk-delay coverage through104

Eight complete600s-per-seed shards at79915ca bind via checkout/header and qualify
325,192 invocations/3,581,807 entries/
1,976 actual100ms dm-delay faults/418,104
exactOk history operations. Writable stores/R5 leader admission/five-second delay/
sync/restoration, all1108 checkpoints and exact raw reports verify. Worst
terminal/progress p99=5.953s/0.851s under original30s gates.
Every actual model's45 dependencies match Git; all canonical members/parts read
back. Captured full workload source/SDK/stores unavailable, integrity/drain
named-test scope. Full200/final-source/full16×200/24h/million-drain remain open.
[Complete recorded-source proof ranges](scale/current-tier3-block-delay-2026-10-04/).

### Explicit-route24h failure and full-profile scheduler comparison

Normal2GiB95b63c0 journal attempt fails7751.58s at checkpoint1050/cutoff29400:
three original20s audits exhaust60s total. Last1040 passes29120 invocations/
321244 entries. First trace uses205 pulls then2869 serial journal reads; bulk
interruption recovery is next. The native-million loading burst overlapped this
failure; resource/server causality remains unconfirmed. SDK/all build fields/
1266 source inputs/6144 original members and parts verify; stores not reopened.
[Complete failure](scale/local-r5-streaming-audit-2026-10-04/explicit-routes-normal-2g-24h-failed/).

The retained300-timer90s candidate remains failed: exact decoding verifies all
300 receipts and original p99/max; all60 >2s intervals intersect recorded outages,
without proving sole cause. A new isolated247eeff million/24h candidate comparison
acknowledges all1M publishes with15m runway/64 publishers/original2s/30s limits and
both all-node SIGKILL cuts unchanged. First/last dueOct4/5 at23:31:49UTC. Live SDK/
all three candidate binaries/all build fields and1693 selected inputs/83 Git inputs
verify. Snapshot proof only; live stores and terminal gates pending. Candidate
remains diagnostic; production adoption/original million/full24h/full matrices
remain open. [Full-profile launch](scale/scheduler-server-candidate-2026-10-04/million-24h-launch/).


### Bounded retained bulk-read recovery — 2026-10-05

The opt-in retained bulk reader recreates transport-interrupted consumers from
its first unvisited sequence, with at most two resumptions under the original
captured cutoff and audit deadline. Semantic errors stay fatal; leader reads
still resolve gaps and short tails. The [focused regression/race/native proof](scale/bulk-read-resumption-2026-10-05/)
qualifies this reader change only. Large native interrupted-tail/R5 and actual
24-hour qualification remain open; the failed explicit-route soak is retained.


The [native R5 100k retained-cohort interruption control](scale/bulk-read-resumption-2026-10-05/r5-100k/)
now qualifies admitted client bulk-resumption at scale:1.2M entries, replacement
cursor at129, no journal point reads, baseline18.777s/recovery19.372s under the
original20s attempt. This is direct retained-cohort checker evidence; longer-soak
scaling, natural fault causes, final-source full matrices and actual24h remain open.
The [new failed Tier2 partition shard](scale/current-tier2-matrix-2026-10-04/partition-1-12-failed/)
retains its missing-terminal-state observation and unconfirmed cause.


### Short successful bulk pulls — 2026-10-05

The installed SDK may return a short batch without an error. The reader now
uses one leader next-message read to establish a retained tail, then bulk resumes
under the same shared two-resumption cap and original audit deadline. The
[regression/race controls and failed native R5 qualification](scale/bulk-read-resumption-2026-10-05/short-success-r5-failed/)
separate the fixed serial fallback from the remaining throughput failure:
short-success recovery visits1,022,814 of1.2M entries before the original20s
budget expires. The native parent remains failed; measure delivery-window and
cursor-replication costs before another soak. No deadlines or release gates relax.

[All-server-kill145–156](scale/current-tier2-matrix-2026-10-04/cluster-145-156/)
now extends accepted recorded-source coverage through156; full200/final-source
matrices and actual24h remain open.


### R5 audit throughput diagnostic — 2026-10-05

The [verified copied-store CPU/phase/window/cursor comparison](scale/r5-audit-profile-2026-10-05/)
reopens the same100k/1.2M cohort with its original logical NATS identities.
Both512-record/R5 attempts hit the unchanged20s budget;512/R1 completes16.813s
and4096/R5 completes15.739s. Production defaults remain unchanged. The next
candidate must bound both bytes and message count, preserve captured cutoffs,
leader gap/absence checks, cancellation and semantic error behavior, and pass
full interruption controls before adoption. Larger known-small-payload windows
and single-replica diagnostics do not qualify worst-case buffering or recovery.
All failed startup/collector attempts remain preserved, and original store files
still match their published archive. Full matrices and actual24h remain open.

### Experimental byte-bounded audit delivery — 2026-10-05

A candidate uses the documented SDK `PullMaxBytes(8 MiB)` client-buffer limit
with `StopAfter(4096)`, sharing the existing captured-bounds scan and two-resume
budget. Production selection remains unchanged. Missing delivery heartbeats
retain their identity and admit transport-timeout recovery; semantic errors do
not. Initial native failure and corrected R3 exact 116-record/30.4MB comparison,
holes, cutoff, cancellation and cleanup are preserved; focused race controls pass.
The SDK byte-mode pointer array adds approximately8MB per iterator, and oversized
records can stall until bounded fallback. Measure actual full100k/R5 latency and
interruption correctness before adoption; the original20s audit target remains.
[Complete evidence and limitations](scale/byte-bounded-audit-2026-10-05/).

### Full R5 byte-bounded retained-cohort comparison — 2026-10-05

Continued verified655-file clone reopens100k completed invocations /1.2M entries
with original logical identities, fresh Docker names and verified R5 cursors.
Byte-bounded4096 candidate finishes complete invariant/state reports17.099s and
15.209s under original20s; both512 baselines deliver all journals but exhaust the
budget before final validation. Actual SDK/five container binaries/639 unchanged
selected Go/module inputs and four CPU profiles retained. No synthetic or natural
fault cut is admitted by this comparison; validate explicit-error and nil-error
short windows on the full cohort before adoption. Defaults and full/24h gates
remain unchanged. [Evidence](scale/byte-bounded-audit-2026-10-05/r5-100k-profile/).

### Full R5 byte-bounded interruption acceptance — 2026-10-05

Verified continued640-file clone /100k completed invocations /1.2M entries,
original20s each, R5 read cursors. Baseline17.824s, explicit-error15.906s and
nil-error short13.956s complete;3.6M journal visits check every sequence exactly
once. Explicit resumes129/zero point reads; short oracle visits129 then resumes130
with one point read. Zero audit consumers required after each case. Named60.17s
PASS in interruption mode requires all reports complete, unlike comparison mode.
Actual executables/639 unchanged selected Go/module inputs/CPU/clone ledgers and
focused race controls retained. Client-suffix loss is deliberately synthetic;
natural transport/leader-loss and semantic-corruption acceptance precede adoption.
Defaults/full-source matrices/actual24h gate unchanged/open.
[Complete proof](scale/byte-bounded-audit-2026-10-05/r5-100k-interrupted/).

### Byte-bounded streaming audit adoption — 2026-10-05

Native R3 actual consumer-leader loss/cancellation and12k/144k large-tail controls
pass original20s attempts with admitted pending tails; compaction/cohort/fresh
state/snapshot/orphan and protocol corruption match point-read oracles. Following
full R5 population/interruption acceptance, all four public streaming APIs select
4096-record/SDK8MiB windows. Public entry-point compatibility passes NATS2.15.0
and2.11.17/R3, including actual legacy executable identity; focused race controls
pass. Actual executables/639 unchanged selected inputs/original stores archived.
SDK pointer allocation/oversized-message caveats retained; point/non-streaming
bulk APIs, original replication, invariant/deadline/recovery limits unchanged.
Existing streaming-state matrix/soak mode uses this materially changed reader;
full final-source matrices and actual24h gate remain open, older failures intact.
[Native proof](scale/byte-bounded-audit-2026-10-05/native-faults/) ·
[Public proof](scale/byte-bounded-audit-2026-10-05/public-adoption/).

### Actual24h qualification relaunched with adopted byte reader — 2026-10-05

Verified disk recovery retains ZIP/canonical/model and failed/live originals.
New isolated e278ffb journal row /seed1 uses original24h/R5/normal2GiB/explicit
routes and20s/60s audit limits with the adopted streaming delivery. Live actual
SDK/all build fields/environment, five server copies/arguments and1284 selected
Git/source inputs verify. Early checkpoint80 completes2240/24724; launch and
snapshot bytes archived, live stores untouched. Legacy Fetch trace does not count
Messages/Next; diagnostic limitation recorded. Native-million campaign shares the
VM. Keep the new unit observed without restarting on timeouts; terminal complete
raw/model/integrity/drain review is still required. No full matrix/24h promotion.
[Launch evidence](scale/byte-bounded-audit-2026-10-05/24h-launch/).

### Complete recorded-source Tier2 all-server-kill200 acceptance — 2026-10-05

Final157–200 shards qualify105252 invocations/1160209 entries/836 faults/
135362 independent operations. Full17-shard aggregation verifies all parts/members,
actual model binaries/45 common source dependencies, exactly200 complete600s seeds
and all three exactOk histories under unchanged30s terminal/progress gates.
Recorded c4fed06 row totals476840 invocations/5256067 entries/3800 faults/
613446 operations; worstp99=18.295s/13.020s. Workload executable/complete captured
source/stores not uploaded; integrity/drain retain named-test scope. This completes
three recorded-source Tier2 rows (journal/consumer/all-server); full13×200/final
source, Tier3/full24h and native-million physical drain remain open.
[Complete raw references and aggregation](scale/current-tier2-matrix-2026-10-04/cluster-full200-qualification/).

### Terminal point-state leader routing — 2026-10-05

The pinned nats.go1.54 modern KV.Get automatically selects DIRECT.GET when
KV_WF_STATE has AllowDirect=true. Direct reads can be served by followers and do
not establish read-after-write coherence. Non-snapshot terminal invariant checks
now use the documented administrative GetLastMsg API, routed to the stream
leader, preserving API prefix/domain, client trace, existing retry/deadline
budgets and DEL/PURGE/expiry-marker absence semantics. Shared initialization is
concurrency-safe; no stream configuration changes.

A synthetic stale-absence wrapper makes the old public Check fail at4.70s.
Corrected real file R3 Check bypasses it, issues two administrative reads and
rejects actual deletion/purge in4.38s. Modern compaction/corruption controls and
legacy2.11.17 compatibility pass; focused race regression passes5.31s. Actual
normal/race SDK identities,641 unchanged selected inputs and original stores
are retained; baseline lacks live identity/before-after capture. This establishes
the point-read correction, not actual follower lag or the historical partition
failure's cause. Fresh snapshot-watch consistency, final-source/full-matrix/24h
qualification remain open. [Complete proof](scale/terminal-state-leader-2026-10-05/).

### Recorded-source disk-delay161-seed coverage — 2026-10-05

Five newly completed shards at79915ca extend accepted continuous coverage to1–156
and separately196–200. Full13-proof aggregation verifies all parts/canonical
members, actual model binaries/45 common Git dependency inputs, raw history
operation counts/exact three-model outputs and600s/30s original gates. All161
accepted seeds total500,752 invocations/5,515,239 entries/
3,059 real faults/643,824 model operations/
1700 cohort audits. Worst terminal/progress p99=
5.953s/0.851s.
Workload SDK/full captured source/native stores unavailable; final integrity/drain
named-test scope. Missing157–195/full200/final-source/full16×200/24h/million drain
remain open; original failed parents are not promoted.
[Complete coverage review](scale/current-tier3-block-delay-2026-10-04/coverage-1-156-and-196-200/).

### Retained native partition replay preparation — 2026-10-05

The Tier2 process harness now accepts optional WF_MATRIX_PROCESS_ROOT to retain
original server binaries/logs/stores after a focused case. It creates a fresh
per-test directory and rejects an existing one; fault scheduling, replication,
runtime, checker and budgets are unchanged. Integration compilation passes.
This prepares a ten-minute seed1 partition replay after the terminal point-read
correction, matching the historical normal-build profile. Native execution and
terminal review remain pending; the historical failure/cause/full gates remain
unchanged. Captured source and actual executable identities are required before
promoting the replay result.

### Captured native partition seed1 replay launch — 2026-10-05

The focused ten-minute partition case is now live at clean93c5133 with normal
SDK/three actual NATS2.15.0 file-replica processes. SDK binary explicitly stamps
the clean Git revision; all live executable hashes/full build fields/arguments/
selected environments and1172 repository Git input bytes verify. Retained roots
are fresh, repeated actual proxy cuts observed; original20s/60s audit,30s p99 and
5m completion limits are unchanged. Normal2GiB/GOMAX2/5G service ceiling and
shared VM with both long campaigns are recorded. Launch archive1193 members/
three parts reads back. Primary stores/unit continue untouched; terminal/source
after/native integrity/model review is pending. No historical causal conclusion,
full-matrix/final-source/24h/million-drain promotion.
[Complete launch proof](scale/terminal-state-leader-2026-10-05/partition-seed1-launch/).

### Corrected-reader native partition seed1 accepted — 2026-10-05

The original ten-minute normal R3 partition case completes at93c5133 in726.25s:
1680 invocations/18573 entries/19 confirmed cuts/six complete checkpoints and2160
exactOk operations under all three independently rebuilt history models. Raw
fault/timestamp/majority/latency metrics verify; worst terminal/progress p99=
19.320s/13.168s under original30s gates, original20s/60s audits and5m completion.
Actual SDK/three native server/model identities,1172 unchanged selected Git
inputs/50 actual model dependency inputs and2314 original store files are retained;
all3528 archive members/four parts read back. First review's older45 dependency
assumption corrected by deriving/verifying the actual graph; both attempts kept.
Final integrity/dispatch drain retain native named-test scope; stores not reopened.
This qualifies the executed-source focused seed, not the historical failure's
cause, full/final-source matrices,24h soak or million physical drain.
[Complete terminal proof](scale/terminal-state-leader-2026-10-05/partition-seed1-terminal/).

### Recorded-source disk-delay174 accepted seeds — 2026-10-05

New157–169 shard passes raw real-device/R5 leader/delay/sync/restoration,
latency/explanation/fencing/cohort controls and three exactOk models. Full14-proof
aggregation reads every part/member, verifies actual model executables/45 common
Git inputs, raw history counts/exact outputs and original600s/30s requirements.
Continuous1–169 plus196–200 totals540036 invocations/5948081 entries/3306 faults/
694332 model operations/1831 cohort audits; worstp99=5.954s/0.851s. SDK/full
captured source/native stores unavailable; integrity/drain named-test scope.
Missing170–195/full200/final-source/full16×200/24h/million-drain remain open;
historical failed parents are unchanged.
[Complete14-proof aggregation](scale/current-tier3-block-delay-2026-10-04/coverage-1-169-and-196-200/).

### Complete recorded-source Tier3 real disk-delay200 row — 2026-10-05

All200 consecutive600s seeds at79915ca qualify. Full16-proof aggregation reads
all parts/canonical members and verifies actual model binaries/45 common Git
inputs, raw history counts/exact three-model outputs, exact seed coverage/19 real
faults per seed/expected cohort audits and original30s gates. Whole row621040
invocations/6840266 journal entries/3800 actual dm-delay faults/798480 exactOk
operations/2104 cohort audits; worst terminal/progress p99=5.954s/0.851s.
This completes two recorded-source Tier3 rows (ahead-clock and real disk delay).
Workload SDK/full captured source/native stores unavailable; final integrity/drain
named-test scope. Full/final-source matrices/24h/million-drain and historical
failed parent verdicts remain open/unchanged.
[Complete200 qualification](scale/current-tier3-block-delay-2026-10-04/full200-qualification/).

### Result-object absence confirmation — 2026-10-05

Worker result transport and client Await now confirm Object Store missing
metadata against the stream leader before treating it as semantic absence.
Confirmed existing metadata permits a bounded modern payload retry; confirmed
missing/deleted metadata preserves ErrObjectNotFound. Explicit administrative
request context preserves cancellation, API prefix/domain and trace routing.
Normal reads and content-hash verification retain their existing behavior;
worker15s and Await5s attempt budgets remain. The pinned legacy GetInfo/factory
context cannot bound administrative metadata requests, so the implementation
uses explicit GetLastMsg context and keeps the modern chunk reader.

Controlled synthetic absence fails baseline public worker/client paths3.71s;
corrected normal4.89s/race6.02s pass, including real deletion/hash corruption,
malformed metadata and blocked oracle deadline/cancel/prefix/domain controls.
Existing large-result replay/Await and missing-reply worker budget controls pass.
Actual executed binaries, selected source snapshots and original R3 fixture files
are preserved in [result absence evidence](scale/result-absence-2026-10-05/).
Snapshot/manifest/other object reads, forced native follower-lag evidence,
historical server-side causality, legacy-server and final-source/full matrix
qualification remain open. Unchanged Tier1 state-machine bodies were not rerun
for this real-transport adapter change.

### Compacted journal snapshot roots read from leader — 2026-10-05

Default snapshot manifest reads now request the state stream leader's latest
value/exact revision before reconstructing a purged journal prefix. Snapshot
object reads use bounded missing-metadata confirmation with the existing cached
Object Store handle. DEL/PURGE/recognized markers remain missing; malformed
manifests and hash/anchor mismatch fail closed. Existing CAS writes, purge
bounds, metadata caches and2s reconstruction/object windows remain unchanged.

A real R3 double-compaction regression with synthetic weak missing/older
manifests fails baseline10.58s, then passes normal7.18s/race7.85s with200 exact
records/revision2/two administrative and zero direct state reads per control.
Native deletion/malformed/oracle deadline-cancel controls, existing compactor,
moving-snapshot, result and purge-lease bodies and journal package controls pass.
One ordinary clean append fixture fails while initially populating objects;
original log retained and exact missing body replay passes3.23s. No claim of
server-side causality or failed parent-group promotion.
[Captured originals and scope](scale/snapshot-manifest-leader-2026-10-05/).

New manifest helper domain/legacy-server coverage, other state/input/blob reads,
natural follower-lag reproduction and full/final-source matrices remain open.
Unchanged Tier1 state-machine bodies are not rerun for default transport changes.
Actual24h campaigns remain at their isolated earlier sources; no restart or
blanket final-source qualification follows from this focused adapter acceptance.

### Recorded-source actual disk-stall seeds1–13 accepted — 2026-10-05

The first complete Tier3 block_disk shard at79915ca independently qualifies
thirteen original600s cases/247 real five-second device-mapper suspensions,
40964 invocations/451209 entries/52668 exactOk history operations/142 cohort
checks. Node4 same writable filesystem/device and both R5 WF_RUN/WF_JRN leaders
are admitted at every cut; blocked sync returns after resume starts. Original
30s gates hold (worst terminal/progress p99=9.044s/0.647s). Three model builds
use45 actual repository inputs matching executed source. Source checkout/API/
header binds the workload; actual workload SDK/full source/store bytes are
unavailable and final integrity/drain remains named-test scope.
[Raw/model/provenance and complete verified parts](scale/current-tier3-block-stall-2026-10-05/seeds-1-13/).
Seeds14–200/current-source/full16-row/actual24h/million-drain remain open; existing
live job and soak handles continue. No native workload is rerun for this review.

### Automatic-membership parent-stack diagnostic — 2026-10-05

Recorded79915ca auto_journal seed1 has an acknowledged Start, captured owner
CAS/rejoined session and four acknowledged missing-journal repair dispatches,
but no uploaded target fetch event before five-minute Await expiry. Complete
raw/API/logs are preserved; consumer/assignment final snapshot, actual SDK/full
source/store bytes and native/runtime cause remain unavailable. No recovery fix
or original failed-parent promotion is claimed.

The active R5 workload now dumps parent goroutines on test failure before cleanup
joins, retaining PID/time/size/truncation. Subprocess workers and earlier startup
failures are outside that dump scope. Normal/race blocked-goroutine controls pass;
a caller-only capture mutant fails. All original workload/audit/latency/deadline
bounds stay. The isolated producer additionally asserts clean VCS-stamped SDK
identity. [Failure and diagnostic originals](scale/current-tier3-automatic-membership-2026-10-05/failed-seed1/).
A fresh single ten-minute diagnostic can establish a new-source verdict and
capture its blocking path on failure; it does not qualify full200/full matrices
or establish this older-source failure's cause.

### Fresh automatic-membership seed1 diagnostic launch — 2026-10-05

Clean isolated cadb346 now runs original10m/R5/seed1/race512MiB/default-route/2m
sync automatic membership with unchanged20s60s30s5m gates. Actual SDK VCS/live
hash/environment and all five server binary copies/arguments verify;1307
selected repository bytes match Git and captured SHA. Original stores remain
live; immutable1332-member/four-part launch facts independently read back.
[Live unit/source/profile and preserved launch](scale/current-tier3-automatic-membership-2026-10-05/seed1-launch/).
First batches and real cuts/rejoins establish execution, not a terminal verdict.
Two existing long campaigns overlap the VM. No single newer-source outcome can
explain the old failed case or qualify full200/full16-row/current-source/24h.


### Terminal byte-reader soak and automatic-membership diagnostic — 2026-10-05

The original e278ffb24h journal row fails after13096.14s at checkpoint1750,
cutoff49000, under unchanged20s/60s budgets. First attempt reaches journal scanning;
report counts are reduced only later and do not measure bytes received. Old trace
omits Messages/Next, so delivery versus throughput remains unresolved. Preserve
all originals and instrument this gap before another unchanged soak. Concurrent
fresh automatic-membership startup/faults and million campaign are recorded;
no causality established. Fresh cadb346 original10m/race/default-route seed1
passes native/producer checks (1764inv/19427entries/19faults), and all2272 history
operations pass three independently rebuilt exactOk models. This accepts only
the focused executed-source seed1. Both complete originals independently read back.
[Failed soak](scale/byte-bounded-audit-2026-10-05/24h-failed-1750/) ·
[Terminal diagnostic](scale/current-tier3-automatic-membership-2026-10-05/seed1-terminal/).


### Healthy byte refill and observable delivery — 2026-10-05

A focused native control separates a reproducible client failure from the
historical unconfirmed soak failures: pinnedSDK1.54.0 PullMaxBytes+StopAfter
stalls a plain8MiB iterator at31/48 quarter-MiB records. Retain one continuous
iterator across4096-record batches with adapter-owned caps. Normal/race healthy
4100-record scans use one cursor and zero gap reads. Recover overlapping replay
only after same replicated memory/AckNone consumer metadata proves a leader move,
within the existing two-resume budget. Both state modes recover1500inv/6000entries
after actual consumer kills with4039 pending records. Original20s/60s budgets stay.
Trace Messages/Next bytes, waits and outstanding calls; retain bounded slow/error
history and forward opaque options. Positive and negative delegation/context
controls pass. Original/intermediate failures and closed native stores preserved.
No1750 historical-cause claim or24h/full-matrix pass; verify100k/R5 performance,
legacy/domain and final-source release gates before promotion.
[Complete evidence](scale/byte-refill-diagnosis-2026-10-05/).


### Full R5 continuous-reader and tracing qualification — 2026-10-05

Executed clean ee4c272:100k inv/1.2M entries, three original20s audits complete
12.501/14.316/13.692s with3.6M exactly-once journal visits and correct interrupted
cursors/leader-point counts. Fixture injects interruption into production
continuous batches. Clean6176205 plain/traced public full audits complete
14.964/14.064s; tracer accounts1.3M records/85.2MB with zero iterator errors.
SDK/live identity/server copies/selected Git inputs/clone ledgers/proof members
verify. This closes focused full-cohort performance/interruption/tracing gates;
actual24h/full-final-source matrices/legacy-domain remain. Shared-VM sequential
numbers are not a stable speedup estimate or historical-cause proof.
Existing million candidate first three-server SIGKILL heals17.092s and receipts
resume, maxlateness17.681s; final physical drain and campaign verdict remain open.
Secure retained-store disk headroom before another growing24h fixture.
[Complete evidence](scale/continuous-byte-r5-2026-10-05/).


### Reboot interruption and fresh corrected soak — 2026-10-05

VM disk expanded to148GiB, about50GiB free at observation. Reboot interrupted
original million-candidate campaign: unit/PIDs absent, last running report341,740,
offline recovery342,191 receipts. Original report/ledger hashes unchanged;
complete originals archived/read back. No completion/drain/candidate adoption.
Fresh cleanb2d7011 original journal seed1/24h/normal2GiB/R5 explicit-route/2m-sync
campaign runs with corrected continuous byte reader and actual delivery tracing,
unchanged20s/60s gates. Live SDK/fullbuild/environment/1,318 Git inputs and five
server copies/routes verify. No terminal24h qualification yet.
[Interrupted original](scale/million-candidate-interrupted-2026-10-05/) ·
[Fresh launch](scale/continuous-byte-journal-24h-2026-10-05/launch/).

Recorded-source79915ca disk-stall seeds1–65 now accepted:198,548inv/2,186,660
entries/1,235 actual5s stalls/682 cohort audits/255,276 exactOk operations across
three models. Complete shards/parts/model dependencies verify. SDK/stores absent
from original uploads, final integrity/drain has named-test scope.66–78 failed;
full200/final-source matrices remain open.
[Complete shard evidence](scale/current-tier3-block-stall-2026-10-05/).


### Initial provisioning boundary evidence — 2026-10-05

At cleanf9aaa5b, retain parent stacks plus concurrent pinned JetStream/route
monitoring for every node after initial provisioning fails. Original45s admission
stays; separate2s observation budget cannot promote a failed verdict. Raw partial
responses/errors and identities survive cleanup. Normal/race cancellation and
partial controls pass; actual five-container paused-monitor control passes13.04s,
eight real responses/four route IDs/two deadline errors. Actual SDK/server copies,
649 Git inputs and728 original archive members verify. Pause admission has
named-test/API-call scope. Original disk-stall75 placement cause remains unconfirmed;
no full/current-source matrix or24h claim. Existingb2d7011 soak continues unchanged.
[Complete proof](scale/provision-failure-observation-2026-10-05/).


### Positive real-domain retirement/reuse — 2026-10-05

Clean9581ebc R3 real WFRETIRE domain clients/servers execute strict continuation
retirement/reuse under race23.44s with original30s startup/60s scenario contexts.
Generation1→3, two old objects collected, old manifest rejected before user code,
one fresh-manifest response lost and repaired, three effects/two terminals.
All peers return fresh2, survivor1 and exact shared/fresh references survive
quiescent GC. Actual race SDK/650 Git inputs/1,076 archive members verify.
This closes the focused positive domain path for this scenario; forced object
absence confirmation, domain server faults, legacy versions, worker SIGKILL,
active-writer GC and final-source full matrices/24h remain separate requirements.
[Complete proof](scale/continuation-retirement-domain-2026-10-05/).


### Domain all-three restart admission failure — 2026-10-05

Real-domain retirement candidate actual race runs at444bd43/506ab50/c0252af
remain failed. Partial records prove all three stopped/new library IDs, but
post-restart account-info reads yield no healed-domain responses under shared5s
observation budget;250ms retries do not close it. Completed-fault counter0 fails.
Fresh workflow result alone does not qualify fault recovery; final peer/integrity
assertions are not reached. Failed SDK/source/stores/raw admissions preserved.
Add local server domain/metadata-leader/running and client connection observations
before cleanup to distinguish fixture routing/reconnect/metadata cases. No server
cause or domain fault/full-matrix/24h pass. Original30s startup/60s scenario stay.
[Failed originals](scale/continuation-domain-all-server-restart-2026-10-05/).


### Domain all-three library restart qualified at original recovery target — 2026-10-05

Clean7a6531f actual racePASS31.78s. Three original servers stop before any new
instance starts; three new IDs and WFRETIRE domain API responses confirm recovery
in5.810s, within30s from cut start including shutdown/restart. Original60s whole
scenario holds. Generation1→3/two collected objects/old-manifest rejection/one
fresh manifest loss/three effects/two terminals/all-peer fresh2/survivor1/shared
references remain strict. Actual race SDK/650 Git inputs/1,082 originals verify.
Added5s metadata gate had rejected connected/running domain nodes before metadata
leadership returned; failures retained, no server-defect attribution. No unchanged
5s rerun. Library restart accepted, not SIGKILL/lease-expiry or full domain matrix.
Legacy/worker SIGKILL/online GC/current-source full matrices/actual24h remain open.
[Complete proof and earlier failures](scale/continuation-domain-all-server-restart-2026-10-05/).


### Retirement/reuse with fresh-manifest worker SIGKILL — 2026-10-05

Clean4ab4bdd actual racePASS35.39s/recovery12.651s under30s; original30s startup,
60s scenario and productionTTL12/heartbeat3/AckWait13 stay. Retire generation1,
collect two old objects while preserving survivor/shared bytes, reuse3 and reject
old manifest. Actual child publishes fresh checkpoint/holds epoch51, is reaped
SIGKILL, successor completes at epoch62 with zero archived-prefix/one frame read.
Three effects/two raw terminals/all-peer fresh2/survivor1/fresh/shared references
verify after quiescent collection. Actual SDK parent/child, three live native
server binaries/build fields,651 Git inputs and1,084 original members verify.
This closes the focused fresh-manifest retirement worker kill combination at
executed source; other publication timings/combined server/domain/legacy faults,
active-writer GC/full current-source matrices/actual24h remain open.
[Complete proof](scale/continuation-retirement-worker-sigkill-2026-10-05/).

### Before-manifest retirement worker crash — 2026-10-05

Kill the fresh-generation worker after a durable frame and StepCompleted but
before manifest publication. Verify manifest absence at admission, exact effect
ledger, successor fencing, recovery under30s with production TTL12, all-peer
results and quiescent GC retention. Replay must reuse the recorded checkpoint
anchor and frame; that frame is reachable and must not be collected as an orphan.
The initial08835e7 fixture wrongly required replacement/collection and failed;
complete originals retained. Corrected8da5935 native racePASS35.61s with12.614s
recovery and epoch51→69; exact effects/all-peer/raw integrity/reachable-frame
retention under quiescent GC verified. This cut
allows journal replay and does not claim bounded resume before manifest repair.

### Recorded-source disk-stall coverage extended to91 seeds — 2026-10-05

Original79915ca successful jobs111324443616/111324443667, artifacts11334565286/
11333539393, seeds79–91/92–104: independently bound API/log/ZIP/raw fault evidence,
494 admitted5s device suspensions, 273 completed cohorts,
102,528 exactOk operations across three rebuilt models/45 executed Go/module
dependencies. Full originals/all members/parts verify. Accepted91-seed coverage
is1–65 plus79–104; failed66–78 and105–200 stay unqualified. No uploaded workload
SDK/physical stores/full source inventory; final integrity/drain named-test scope.
Original full200/current-source/full matrices and24h remain required.

### Protobuf continuation worker crash with JSON successor — 2026-10-05

Extend the retirement/reuse worker-SIGKILL case with protobuf-v1 fresh-generation
writes and a JSON-writing successor. Admit actual retained protobuf checkpoint
anchor bytes before killing the held owner after manifest publication. Require
raw JSON terminal bytes, higher successor epoch, bounded checkpoint resume,
exact effects and all-peer result/integrity/GC checks under original deadlines.
The new journal constructor combines existing encoding selection and supplied
snapshot transport so fault injection does not replace the selected writer.
Native07a419c racePASS35.43s/recovery12.747s/epoch51→62; actual protobuf
anchor independently decoded by generated Python codec and successor JSON
terminal decoded. One frame/zero archive/exact effects/all-peer/raw integrity/
quiescent GC checks pass; full protobuf rolling/chaos remains open.

### Protobuf journal repair before manifest publication — 2026-10-05

Add the before-manifest counterpart of the mixed-encoding worker crash: confirm
the durable protobuf StepCompleted and prepared frame while no manifest exists,
reap SIGKILL, then require a JSON writer to replay the journal, repair the same
frame and finish with a higher lease epoch. Exact effects, all-peer/raw integrity
and quiescent retention remain mandatory under original30s recovery/12s TTL/60s
scenario. Read counters are unobserved for this cut; no bounded-resume claim.
Native de673be racePASS34.62s/recovery12.846s/epoch51→69, exact effects,
all-peer/raw integrity/quiescent retention verified. Independent generated Python
codec binds actual protobuf completion to prepared/repaired frame; JSON terminal
decoded. Full rolling/chaos remains open.

### Fresh full-profile timer candidate after VM reboot — 2026-10-05

The earlier million-message/24h diagnostic stopped in the VM reboot, with342,191
receipts recovered offline; original stores/report/ledger retained unchanged.
Fresh clean68d69c0 comparison now runs in an isolated source/campaign root with
actual retained program and allthree candidate servers/live executables/source
inputs independently verified. Original1M/24h/64publishers/15mlead/p99≤2s/max≤30s/
two all-serverSIGKILL/physical replica drain targets unchanged. Full publication
and terminal outcome pending at launch. This candidate is still rejected by the
release verifier; no production source pin or default changed. Terminal review
and adoption qualification remain required. Shared-VM overlap with runtime24h
recorded, not attributed as cause of any performance result.

### Sustained mixed protobuf-to-JSON worker replacement profile — 2026-10-05

The opt-in worker_kill profile `--journal-rollout protobuf-to-json` starts five
protobuf-v1 writers; every replacement generation writes JSON. Preserve original
five-second kill cadence, lease/fencing, six workload cells, ten-minute/24h scopes,
checkpoint audits and latency/integrity/history/drain gates. Final audit captures
actual raw retained entries per invocation and requires at least one invocation
with protobuf initial-owner and JSON successor writes. Independent generated
Python codec verifies every wire format against admitted worker generation/PID,
counts and mixed-invocation admission. Row verifier requires explicit profile
flag and rejects promotion as the default writer profile. Go format controls,
Python positive/six corrupt-missing controls and wrong-row producer rejection
pass; native profile qualification pending. Needs protoc/Python protobuf for
independent decoding. Full protobuf rolling/chaos and current-source matrices
remain open; this profile is additive and does not replace an original row.

### Mixed-encoding worker replacement admission smoke accepted — 2026-10-05

Cleanbb79fa2 native normal seed1/35s smoke passes140.67s including startup/cleanup:
56inv/621entries/6SIGKILL, three mixed-owner invocations. Independent Python codec
decodes266protobuf/355JSON worker entries and checks admitted generation/PID.
Actual parent/five then-running SDKs/1,333 selected source inputs verify; three
rebuilt history models exactOk72operations/52Git dependencies. All5,206 original
and70outer archive members/parts verify. Native broker executable/store provenance
is named-test scope, not independently captured live-container binary hashes.
Smoke only; ten-minute/24h mixed-encoding, default-profile/current-source matrices
and full rolling/chaos remain open. No deadlines or fault gates changed.

### Sustained mixed-encoding launch identity verified — 2026-10-05

The original ten-minute worker_kill profile now runs at cleanab5cd71 with explicit
protobuf-to-json rollout, five-second kill cadence and unchanged workload/audit/
lease/liveness/p99/drain targets. Actual parent SDK/17 captured SDK processes,
allfive live native container executables/full buildfields and1,334 selected
Git source inputs verified; all1,350 immutable launch members/fourparts read back.
A continuous SDK observer records later generations; final coverage pending.
Real accepted smoke artifact is rejected by default-profile verifier without the
rollout flag, with no report created. Terminal sustained/wire/history/integrity/
drain review and full rolling/chaos/matrices/24h qualification remain required.

### Live candidate ledger observation without interrupting the campaign — 2026-10-05

A separate immutable snapshot of the running candidate report/receipt ledger
independently validates8,755 checksum slots/unique sequences/exact deadlines/no
server-or-client early timestamps; snapshotp99/max0.691/2.053s. Actual live program
and allthree initial candidate servers stillmatch retained hashes/buildfields.
This does not stop/mutate the original or reopen its stores; report/ledger are
not an atomic snapshot and copied slots do not independently prove fsync.
All9 snapshot members/part verify. Original campaign remainsrunning; full1M/
24h/two restarts/raw latency/physical drain/adoption stayopen. Running final
stream/ack-pending zeroes are unobserved placeholders, never drain evidence.


### 2026-10-05 terminal follow-up: original gates remain open

The corrected continuous-byte journal 24h run at b2d7011 failed checkpoint1040 /
29120inv after7986.85s. Attempts2/3 return321442/321543 journal records withzero
journalNext errors and reach consumer cleanup; initial state watch is untraced
and precedes the absent Keys call. Diagnose this phase under the existing20s /
60s / three-attempt budget before another long run. Cause remains unconfirmed.
[Full failed originals and independent trace comparison](scale/continuous-byte-journal-24h-2026-10-05/failed-1040/).

The opt-in protobuf-to-json ten-minute worker_kill run at ab5cd71 failed the
original strict30s terminalp99 gate: shortworkflows33.911s,392inv/4345entries/
119faults. Preserve and inspect individual latency/fencing history before rerun;
accepted35s smoke does not qualify this sustained case. Both failed originals
and selected executed-Git inputs are independently verified. SharedVM overlap
with each other and the still-running million candidate is recorded, not causal.
[Full sustained failed originals](scale/protobuf-json-worker-rollout-2026-10-05/10m-failed/).

The next source reports initial-state-watch failure phase and delivered-prefix
counts while preserving underlying retry errors and all original budgets.
Focused state-snapshot tests pass; no native soak rerun or cause/fix claim.


### 2026-10-05 focused diagnostic follow-up

Healthy restored copies of checkpoint1040's five stores deliver all29177 state
values within0.131–0.295s, including both existing2s wrappers. Retained cardinality
alone does not reproduce the timeout; no production deadline change is justified
by this result. Live-fault/watch-progress diagnosis remains open. Source886de67,
actualSDK/five server builds/fullcopied-store proof and original before/after
hash equality retained.
[Complete diagnostic](scale/state-snapshot-copied-2026-10-05/).

The failed sustained rollout's392inv/4345rawrecords independently decode,
including sevenmixedwriterinv. Its sole ≥30s short terminal is entirelyJSON and
loses two consecutive owners toSIGKILL before a third completes delivery4;
delivery2observedleaseheld/NAK. Next add this concrete repeated-owner-loss sequence
to seeded simulation and analyze lease/ack/redelivery clocks before sustained
rerun. Original strict30s remains; this is separate from the closed single-kill
TTL30 mismatch. Parent remainsfailed, observercoverage110/124PIDs.
[Bound original wire and timeline](scale/protobuf-json-worker-rollout-2026-10-05/10m-outlier-review/).


### 2026-10-05 seeded repeated-owner-loss sensitivity

Addedworkload at e63bb02:100000normal/1000race seeds completed, nine combinations,
392 exact racepins and crossprocessseed42 trace. An explicit delayedfirstexpiry
hypothesis adds observedhelddelivery/5sNAK thenexposuretosecondownerloss; modeled
completion34.1–34.5s vs15.6–16s withidealexpiry. This usesproductionlease/journal
andmodeleddispatchports, notfullworker/heartbeat/NATSexpiry. Persistentexpiry
lagaddsanotherheldretry anddiffersfromnativeoneheldtimeline. First-generation
lag plusmodeledservicecost/cut/pollclocks areassumptions, nothistoriccausality.
ProductionTTL12/AckWait13/strict30s unchanged; failednativeparent remainsfailed.
Next retainheldentryrevision/epoch/creationtime andoperationtiming without
additionalbrokerreads, distinguishrenewal/replicaexpiry/delivery beforeanother
sustainedrun. Prior121workloadqualification remainsatitsrecordedscope; theadded
workloadand392pins do notaloneprovefullcurrent-sourcegate.
[Full model proof](scale/delayed-expiry-worker-kills-2026-10-05/).


### 2026-10-05 held-entry diagnostics prepared and verified

Atad92227, optionalobserver reports already-read heldentryrevision/epoch/worker/
servercreated/clientobserved with explicitmissing/race/malformed scope, and adds
it onlytolease_held dispatch events. No additionalbrokerrequest, Acquire/error
identity/timingunchanged. Sixleasecontrols/worker controls, nativeR3file12sTTL
renewalmetadata(PASS2.66s),1000held-terminalraceseeds/all392pins pass. ActualSDKs/
1049Gitinputs/full1122memberproof retained. NativeNATSembeddedinSDK; preliminary
fullprovisiontimeout source/reportonlyretained (noactualSDK/stores), unqualified.
Next observe a sustained original worker-kill rollout with these fields to
separate renewal/expiryvisibility/delivery effects. Strict30s remains; this
instrumentation doesnotresolve historicalcause orqualifyfullmatrices/actual24h.
[Full focused observer proof](scale/held-lease-observation-2026-10-05/).


The original ten-minute workerkill rollout nowruns at97a037e withnoadditional
brokerreads andunchangedstrict30s/audit/integrity/drain gates. ActualSDK143072/
17observerrecords/fiveDockerprocessbuilds/1355Gitsourceinputs andfull1372member
launch proof verify. Early336heldentryrecords include two client-observed minus
server-created ages≥12s (max12.185s), independently recalculated. These are
clockdifferences, notserverexpiry authority orhistoriccausality. Wait for actual
terminal result andretain all originals, compareheldmetadata/leaseoperation/
ownerdeath/redelivery timing before proposing a runtime fix. Overlaps live
millioncandidate; observercoverage/default/fullmatrix/24h remain pending.
[Immutable launch and early held entries](scale/held-lease-observation-2026-10-05/10m-launch/).


### 2026-10-05 sustained mixed-encoding and state-watch terminal review

The instrumented ten-minute opt-in worker_kill seed1 at97a037e qualifies its
recordedscope:PASS641.43s/840inv/9268entries/119kills/threecohortaudits, worst
terminal/progressp99 17.975/13.258s. Independent747protobuf/8521JSON/29mixedwire,
three rebuilt exactOk1080operationmodels/52Gitdeps, actual124workerSDKs+parent/
1355Gitsourceinputs/all6997originalmembers verified. Finalheldages12.185s do not
establishserverexpiry orhistoricalcause; earlierfailedparent remainsfailed.
No defaultwriter/fullrolling-chaos/fullmatrix/actual24h claim.
[Qualified sustained profile](scale/held-lease-observation-2026-10-05/10m-qualified/).

The copied29177state consumerleader-loss diagnostic at7a70440 fails after
11685updates/nobarrier, three2s calls exhausted6.001s; actualR1memoryconsumer/
pending27058/node3SIGKILL observed. AllactualSDK/serverbuilds/source/copy/archive
verified, originalsunchanged; killedpostcutlog unavailable/secondarycleanupfail.
Its clientpolicy differsfromactualmatrix (discovery/dialtimeout/reconnectwait).
Correctfixturepolicy andretainconnection/metadata/attemptobservations before
changingruntime; failuredoesnotprovehistoricalcheckpoint1040cause. Original
20s/60s/threecheckpointattempt gatesremain.
[Full failed copied-state proof](scale/state-watch-leader-loss-2026-10-05/).

### 2026-10-05 corrected copied watch recovery and audit trace coverage

The copied-state leader-loss case at06a34af passes at the original budget:
first2s watch expires after11685values/noinitialbarrier; second completes all29177
values, total2.241s/native14.49s. Node0 R1memory consumer had27221pending before
observedSIGKILL. SDK/fiveactualserverbuilds/657Gitsourceinputs/3706originalstore
files and4194archive members/eightparts independently verified. Original bytes
unchanged. Client policy matches the real matrix; its five mapped endpoints and
empty discovered pool were observed after recovery. Different killed leader and
unobserved previous pool prevent discoverycause attribution. Historical1040/
fullcombinedaudit/matrices/actual24h remain open.
[Focused proof](scale/state-watch-leader-loss-2026-10-05/mapped-client-qualified/).

The opt-in retained-audit tracer now delegates WatchAll creation and WatchStop
cleanup, preserving the original context/options, update channel/buffering,
constructor results/error identity and each Stop call. It adds no update relay,
extra broker requests, retry or deadline. Successful cleanup does not certify
an initial completion barrier. Focused delegation/trace controls pass; this is
preparation for a changed diagnostic, not a successful native soak or cause fix.

A new opt-in copied-checkpoint1040 diagnostic runs the complete streaming
journal/state cohort checker through invocation sequence29120, under the original
20s single-attempt budget and actual matrix client policy. It records the full
report and creation/cleanup trace. Original stores must be independently verified
and copied before launch. Its native result is pending; it does not recreate
concurrent faults or qualify the historical failed soak.

### 2026-10-05 complete failed-soak copied cohort audit accepted

The full checkpoint1040 cohort at executed e8bce87 passes in5.184s/native17.27s
under original20s attempt:29120inv/journals/terminals,321151entries. Complete
freshjournal delivery321746records/48299508bytes/zeroNext errors; an initial2s
KVlookup retried, WatchAll andcleanup pass. ActualSDK/fiveDockerbuilds/659Git
inputs/3706originalstorefiles/4197archive members/nineparts independently verify.
Originalstoresunchanged. Historical attempts2/3 had only1.861/0.863s remaining
afterjournalcleanup, bounding subsequentwatchbudget by source inference;
originalwatchwait/initialbarrier unobserved. No budgetchange, pressurecause,
historicalfix/fullmatrix/24h qualification. Next observe complete phases under
concurrent faulting workload rather than rerun unchanged instrumentation.
[Full copied audit proof](scale/failed-soak-copied-cohort-2026-10-05/).

### 2026-10-05 pre-deadline audit observation prepared

Opt-in `--audit-wait-stack` requires retained audit tracing and captures the
parent SDK goroutines plus current trace one second before a pending attempt's
existing deadline. Completed/cancelled calls skip capture; actual observer errors
are retained, no capture produces no error wrapping. The source preserves20s
attempt/60s total/three attempts and original fault gates. Parent-only snapshots
are not child stacks, failure verdicts or historicalcause proof. Normal diagnostic
controls and race checks verify cancellation, pending trace, actual blocked parent
stack, metadata deadline and capture error identity. The producer clears inherited
activation and records the explicit flag; arguments and verifier gates unchanged.
Native changed-instrumentation actual24h observation remains pending.

### 2026-10-05 changed-instrumentation actual24h journal launch verified

Executed aace912 original24h seed1 normal2GiB/GOMAX2/explicitroutes/2msync/
streamingstate profile started with pending-audit observation. ActualSDK162125/
supervisor161783 live, parentenvironment/fiveDockerprocessbuilds/routes/mounts/
1374Gitinputs/1393immutablearchive members/fourparts independently verified.
Firstthree cohort audits passed; terminal pending. Parentonlywatch/stack capture
fills prior observation gap, no historicalcause/fix, budgetchange or fullmatrix/
24h qualification. SDKobserver live; launchsnapshot doesnotcover allfuture
serverreplacements. Candidate million sharesVM; no pressureattribution.
[Complete launch proof](scale/watch-observed-journal-24h-2026-10-05/launch/).

### 2026-10-05 recorded-source disk-stall143 seeds accepted

New executed79915ca shards105–156 qualify after originaljob/artifact/log binding,
strictfault/latency/cohort checks and three rebuiltmodels exactOk202186operations
with45actualexecuted-source dependencies; reviewhelper Gitidentity also retained.
Allfour1398memberarchives andparts readbackverified. Combinedaccepted1–65 and
79–156:143seeds/435540inv/4796595entries/2717faults/1488cohorts, worstterminal/
progressp99 9.530/2.980s. Failed66–78 stayunqualified,157–200 running/queued.
No additionaldispatch/failedparent promotion. Actual workloadSDK/fullsource/
physicalstores were notuploaded; finaldrain named-test assertion scope. Fullrow/
current-source/fullmatrix/24h stayopen.
[Complete ledger](scale/current-tier3-block-stall-2026-10-05/).

### 2026-10-05 native lease loss with a cancellation-ignoring effect prepared

A retained opt-in variant of the existing real R3 lost-renewal-ack worker proof
keeps the old SDK step effect blocked despite cancellation, requires the worker
loop to stop and a successor to finish before releasing the old effect's stale7
result, then requires all three peer journals/results to remain unchanged at42.
The ordinary cooperative test retains its transport/request sequence. Native
50s fixture context,12s lease TTL/defaultAckWait and strict30s recovery from
observed fencing are unchanged. Actual committed lease renewal/revision, partial
and terminal journal, proxy traffic, peer verification, integrity and stopped
stores are retained. Callback context cancellation does not forcibly terminate
external code; effect idempotency remains necessary. Raw outer handlers blocking
outside SDK effects are not covered. Native opt-in execution/review pending.

### 2026-10-05 native cancellation-ignoring SDK effect qualified

Executedee2f53d nativeR3 racePASS14.96s/recovery1.210s fromobservedfencing,
strict30s/TTL12/AckWait13 unchanged. Nativelease renewalrevision4→5 committed,
correspondingPubAck absentfromuntruncatedforwardedproxytrace. Oldeffect remains
blocked through workerstop and successor epoch7 terminal42, then returnsstale7;
allthreepeer journal/results unchanged, twoeffects/fourrecords/oneterminal/
raw integrity pass. SDK/661Gitinputs/3287selectedexternalinputs/4350archive
members/twoparts independently verify. Servers embeddedinactualSDK/NoLog;
queue/allpeer checks named-test scope/stoppedstores notindependentlyreopened.
This closes the focused native SDK step-effect late-result case. Raw outer
handlers outside SDK effects, combined process/partition cuts, fullmatrices/24h
remain open. No productiondecisionchange or unchanged simulation rerun.
[Complete native proof](scale/ignored-effect-lease-loss-2026-10-05/).

### 2026-10-05 full PostgreSQL projection fault proof prepared

The original50k projection-process crash case now has an explicit PostgreSQL
variant: actual projection SDK verified/reapedSIGKILL, all50000 results checked
while it is down, exact100000journal-message lag, then a confirmed SQL writer
backend termination and library journal-leader stop/restart during partial SQL
catch-up. After the faulted writer exits and allthree journal replicas are current,
a replacement drains lag and fullRebuild must preserve every exposed SQL row
and indexed type/id/status/attribute column byte-for-byte. Internal random rebuild
generation tokens intentionally change and are excluded from exposed state.
Original20minute fixture budget/defaultcount50000 maintained; optional smaller
count remains diagnostic. DefaultKV proof retains its behavior. Native run and
independent raw/source/process/store review are pending. No SIGKILL claim for
in-process library servers, PostgreSQL server crash or actual24h qualification.

### 2026-10-05 full PostgreSQL projection combined fault failed

Executed f87c422 nativeFAIL405.60s under original20m/default50000. All50000
results checked while observed/reapedSIGKILL projection was down; lag100000.
SQLwriter backend termination and library journal-leader restart admitted at31
rows; faulted writer exits lostsession, allthreejournal replicas current.
Replacement later fails noresponders, before lagzero/rebuild equality. Worker
cleanup also reports closedconnections; libraryrestart replaces pinnedclients.
Replacement used survivorclient and wrote rows, so primarycause unconfirmed.
Independent actualSDK/PostgreSQLexe/662Gitinputs/3287selectedexternalinputs/
1281closedSQLmedia files and7609archive members/threeparts verified. Preserve
failure, correct fixtureclient/worker lifecycle and readiness, add dependency
trace before changed fullrun. No recovery/currentmatrix/24h qualification.
[Complete failed native proof](scale/postgres-projection-fault-50000-2026-10-05/failed/).

### 2026-10-05 PostgreSQL projection fixture lifecycle correction

The next full50000 PG case requires a physically drained workflow queue and joins
allsix completed workers before the projection-only fault, refreshes pinned JS
handles after libraryRestartNode, and requires R3-current WF_INV/WF_JRN/
KV_WF_STATE/WF_PURGE sources before replacement construction. DefaultKV child
backend is explicitly selected to avoid inherited PG configuration. Bounded
trace delegates original projection source reads, orderedconsumer creation,
consumerInfo/Fetch/nativebatch errors and journalreset with original arguments,
contexts/results. PureGo trace wrappers are shared across platform fixtures.
Original20m/default50000 and failure semantics unchanged; no added projection
retry or primarycause claim. Full changed native result pending.

### 2026-10-05 recorded-source disk-stall156 seeds accepted

New157–169 nativejob success bound to actual artifact/log/ZIP; original fault,
latency and cohort gates regenerated. Three rebuilt history models exactOk51552
operations/45 executed79915ca dependencies, helperGitidentity retained; all1398
completearchive members/threeparts verified. Combined1–65 and79–169 (156seeds)
475636inv/5238317entries/2964faults/611542modelops/1626cohortaudits, worstterminal/
progressp99 9.743/2.980s. Failed66–78 stayunqualified; remaining170–200 live.
No workloadSDK/fullsource/physicalstore upload; finaldrain named-test scope.
Full200/final-source/fullmatrix/24h and failedparent remainopen.
[Complete156-seed ledger](scale/current-tier3-block-stall-2026-10-05/).

### 2026-10-05 corrected PostgreSQL full fault locates ordered Fetch failure

Executede6c124f full50000/nativeFAIL494.71s/original20m. Allresults/lag100000;
workflowqueue drain/allsix workers joined beforefault, SQLbackend termination
at78rows and libraryjournalnode2 restart confirmed. Refreshedclients and allfour
R3 projection sources current. Faultedwriter exits API503/10008; replacement
boundedtrace locates fatal WF_INV orderedFetch(256)/noresponders aftercatchup.
No workercleanup errors; lagzero/rebuild equality not reached. All7610archive
members/threeparts/actualSDK/PostgreSQLexe/selectedinputs/closedSQLmedia verified.
CapturedSDK source resets orderedconsumer on everyFetch and recommendscontinuous
Messages/Consume; sourceobservation not servercause proof. Investigate continuous
iterator/context/cleanup controls before changedfullrun; no unchangedrerun,
smallerpopulation qualification or historicalfailure promotion. Fullgates open.
[Complete refreshed failed proof](scale/postgres-projection-fault-50000-2026-10-05/refreshed/).

### 2026-10-05 continuous PostgreSQL rebuild source reader

The source now keeps one ordered Messages iterator with a256-message buffer,
originalretainedinput watermark and context-driven stop/join. It no longer
recreates an ordered consumer on eachFetch batch. SDK iterator recovery handles
consumer deletion; surfaced permanent/iterator/noresponder failures remainerrors,
not an empty-source pass. Quiet reads require zero serverpending AND equality of
serverdelivered and actuallyobserved sequence, preventing an in-flight delivery
from certifying completeness. Native R3file control deletes the actual consumer
and recovers exactly600 retainedinputs across real sequenceholes, racePASS5.91s;
1000 seededhole/watermark controls and errors/cleanup/cancellation controls pass.
Initial nativecontrol failed beforefault at streamcreation; source/log retained,
corrected with boundedreadiness under same30s control deadline. Full50000 original
20m combinedcase stillneeds changedsource qualification; historicservercause and
failedparents unconfirmed/unpromoted. DefaultKV path unchanged.

### 2026-10-05 independent full projection retained audit prepared

A standalone review helper opens only fresh copies of closed three-node
retained stores. It observes existing R3 INV/JRN/RUN/STATE readiness without
provisioning, then applies the complete streaming retained-state checker under
original20s singleattempt: exact50000inv/journals/terminals and100000entries.
It also requires physicalzero WF_RUN on allthreepeers and zero pending/ackpending
on every partition durable, under that same review context. External producer
must bind source/executable and original/copy byte hashes before and after the
review. Helper compiles; copied50k nativeexecution pending. This is an independent
raw-state gate, not a substitute for original projection recovery/rebuild verdict,
concurrentfaults, historicalcauses, fullmatrices or24h. No originalstore reopen.

### 2026-10-05 recorded-source disk-stall161 seeds accepted

New196–200 actual nativejob success/metadata/log/ZIP bound; originaldevicefault,
latency and cohort gates regenerated. Three rebuilt models exactOk19476operations/
45 executed79915ca dependencies; all630archive members/twoparts verified.
Combined1–65,79–169,196–200 (161seeds):490784inv/5405195entries/3059faults/
631018modelops/1676cohortaudits; worstterminal/progressp99 9.743/2.980s.
Failed66–78 stayunqualified; remaining170–195 live. No workloadSDK/fullsource/
physicalstore upload, finaldrain named-test scope. Full200/final-source/fullmatrix/
24h and failedparent remainopen.
[Complete161-seed ledger](scale/current-tier3-block-stall-2026-10-05/).

### 2026-10-05 full PostgreSQL combined projection recovery and raw integrity qualified

Executed9fbfa16 nativePASS1030.97s/original20m/default50000. Observed/reapedSIGKILL
projection down while all50000results checked; lag100000. SQLwriterbackend terminated
at81rows, actuallibraryjournal leader stop/restart; refreshedclients/four R3 sources
current. Continuousreplacement drains lagzero; fullrebuild preserves every exposed
SQLrow/indexedcolumn byte-for-byte, all50000 canonicalrows independently parsed.
ActualSDK/664Gitinputs/3287selectedexternalinputs/PostgreSQLexe/1284closedSQLmedia/
all7617archive members/fourparts verified. Fullcopyaudit uses separatelycaptured SDK
and only byte-identical closedstore copies: 50000inv/journals/terminals/100000entries,
audit5.490s/whole5.531s underoriginal20s; allthree physicalqueues and64partition
durables zero. All2352 originalfiles unchanged/1664selectedinputs/4035archive
members/twoparts verify. Historicalfailedparents/causes remainunconfirmed; no
PostgreSQL serverSIGKILL/currentmatrices/24h qualification. InternalrandomSQL
rebuild generations excluded from exposed-row comparison. Libraryservers embedded/
NoLog, selectedprovenance excludes exhaustivecompiler/assembly/embed inputs.
[Complete native and full copied-integrity evidence](scale/postgres-projection-fault-50000-2026-10-05/).

### 2026-10-05 combined full fanout parent/journal boundaries prepared

A retainedoptin six-case full500child fixture combines actualparent SIGKILL at
first/interior/last creation and resultcollection positions with a libraryjournal
leader stop/restart at the same boundary, before successor execution. Eachcase
keeps original5m/500children/prefix-preservation/higher-epoch/fullintegrity gates.
It retains observedchild SDKidentity/buildfields/prekillprefix, restartedserver
identity/stream beforeafter and finalcounts/results. Resultphase joins completed
outside-child loops before replacing clients, then refreshes journal/client
handles for successor reads. Defaultisolated fixtures retain their behavior.
Compiled optin skipped; native combined six-case execution remains pending.
No serverSIGKILL/fullfaultcrossproduct/fullmatrix/24h qualification claimed.

### 2026-10-05 combined fanout acceptance guard

The six-boundary result guard now has an explicit combinedmode requiring the new
actual test identity, all six terminalpasses, one journalrestart marker percase,
correctphase/node/messagecount and strict parentSIGKILL→journalrestart→preserved
prefix order. It retains exactpositions/childcount/higher-epoch and refuses full
release promotion. Six Python controlmethods pass; missing/skipped/failed/duplicate
execution, isolatedrun substitution, missing/reordered/wrongphase/wrongnode/wrong
count/repeatedfault and prefix/epoch/count mutations rejected. Defaultisolated
mode unchanged. Combinednative six-case terminal proof remains pending.

### Recorded-source disk-stall coverage through174 seeds — 2026-10-05

Native seeds170–182 accepted after original job/artifact/log binding,
regenerated device fault/latency/cohort checks and all three separately rebuilt
history models:52452 new operations/45 executed-source dependencies. Full1398
archive members/three parts readback verified. Combined seeds1–65,79–182 and
196–200 total174/531580inv/5854582entries/3306faults/683470modelops/
1818cohortaudits. Failed66–78 remain unqualified;183–195 pending. Qualification
is only at executed79915ca. Original runner did not retain workloadSDK,
exhaustive source inventory or physical stores; final drain named-test scope.
Full200/current-source/full matrices/24h and failed parent remain open.
[Complete174-seed ledger](scale/current-tier3-block-stall-2026-10-05/).

### Combined500-child boundary matrix failure preserved — 2026-10-05

Executed e20cf89 race matrix failed1046.21s. All three creation boundaries passed;
first/interior result boundaries admitted both faults then hit original five-minute
deadline, last result did not reach its cut within original30s. ActualSDK/source
before-after/childSDK/restart proof verified;16913 archive members/threeparts
readback. Failed matrix remains failed. Causes unconfirmed, no unchanged rerun;
read-only fresh copied-store diagnosis prepared. Snapshot-purged count cannot
bound reconstructed prefix length; require tail-sequence observation plus exact
prefix equality in future guard instrumentation. Original gates unchanged.
[Complete failed matrix proof](scale/fanout-combined-boundaries-2026-10-05/).

### Combined fanout outside-child admission correction — 2026-10-05

Three actual SDKs opened only fresh closed-store copies. First/interior/last
parents await an incomplete outside child;496/496/493 of500 child terminals,
4/4/7 outside children incomplete. Original files remain identical; exact helper/
1655selected inputs/3951archive members each/four total parts verified.
Aggregate WF_SIG count includes parent-partition children, so it cannot admit
outside-child worker shutdown. Changed fixture confirms every outside result
before worker stop. Restart trace/checker cover captured prefix tail and stream
tail sequences across purging and require unchanged restart count/tail, original
prefix equality and higher successor epoch. Allnine acceptance controls passed;
race compile/opt-in skip passed1.083s. Original500children/five-minute case and
30s cut-admission budgets unchanged. Full changed matrix pending; original failed
matrix stays failed, no production decision or historical server-cause claim.
[Complete diagnostics and correction](scale/fanout-combined-boundaries-2026-10-05/).

### Reproducible combined fanout campaign producer and CI — 2026-10-05

`scripts/run-fanout-combined-test.py` ports the retained local campaign to a
fresh absolute root outside a clean checkout, selectable int64 seed, unchanged
500children/six first-interior-last boundaries/five-minute cases/35-minute native
limit/race/GOMAX2/2GiB. Captures exact producer Git bytes, selected Go/module and
external inputs before/after, actual parent executable and native outcome,
commands and alloriginals. Manual `fanout-combined-boundaries.yml` runs nine
acceptance controls and the full producer, then requires allsix combined passes;
always uploads the whole root including hidden files and SDK/store bytes.
Source and workflow syntax checked. Workflow not dispatched/qualified yet;
corrected local actualSDK342837 at161dfa9 remains live and separately bound.
No full fault matrix or24h promotion.

### Confirmed-outside fanout matrix: last cut still missing — 2026-10-05

Executed161dfa9 native raceFAIL729.17s. Creation allthree and results first/
interior passed500children/result249500/prefix/higher epoch; outside489results
confirmed before stopping workers. Interior prefix2227/tail4211 survives while
live messages2178. Last result cut not admitted within30s; completeactualSDK/
source/five childSDKs and16656archive members/threeparts verified. Freshlastcopy
has496terminal children; four incompleteallparentpartition52, parent Suspended
waitingon one,493pending/1ackpending. Original2086files unchanged,1655inputs/
3951members/onepart verify. Outside admission corrected, remaining processing
or blocked-call cause unconfirmed; prepare opt-in operation/dispatch timings
and predeadline stack before a changed diagnostic. Original500/five-minute/30s
limits unchanged. Whole matrix remains failed; no release/physicaldrain claim.
[Complete failed proof and copy diagnosis](scale/fanout-combined-boundaries-2026-10-05/).

### Predeadline fanout parent observations prepared — 2026-10-05

Opt-in `--diagnostic-trace` records child dispatch and per-operation timings,
actual child SDK identity immediately after start (also on non-admitted cuts),
and a bounded complete/truncated goroutine stack at29s. Original30s marker
budget begins before capture; five-minute case unchanged. Default profile has
no trace callbacks or timer. `--case results/last` selects a focused full500-child
diagnostic and labels metadata accordingly; it cannot qualify the six-boundary
matrix. Allnine existing guard controls pass, trace integration race compile/
opt-in skip passed1.059s. Prepare one changed-observation last-case run to locate
partition52's four missing children/493pending; do not promote focused result,
rerun the unchanged full graph or relax the deadline.

### Fanout shared-partition child preparation and signal barrier — 2026-10-05

Focused9e759ae lastcase FAIL95.28s; predeadline trace shows1298journal appends/
489signal reads/sixparent deliveries/nochild dispatch, snapshot JSONdecode at29s.
ActualSDK/producer/666Gitinputs/3287external inputs and6065archive members/two
parts verified. New result fixture prepares all500child results, including shared
partition, through production workers with bounded concurrency4 and parent
handler held before SDK collection. Joins workers; separate production signal
barrier consumes all500names, checks no earlyparent terminal or SDK result.
Then realcutworker collects normal500SDK results at exactfirst/interior/last
boundaries. Creation profile unchanged, five-minute case and30s marker unchanged.
Ten acceptance controls pass; race compile/opt-in skip1.058s; full changed run
pending. No default production decision/timing change, historical servercause,
failedparent, full faultmatrix or24h promotion.
[Complete trace and changed preparation](scale/fanout-combined-boundaries-2026-10-05/).

### Recorded-source disk-stall successful coverage complete at187 seeds — 2026-10-05

Final183–195 job111324444117/artifact11353020473 accepted after exactmetadata/
originalfault/latency/cohort regeneration and three separately rebuiltmodels
exactOk49644operations/45executed-source dependencies. All1399archive members/
threeparts readback. Combined1–65 and79–200:187seeds/570192inv/6279811entries/
3553faults/733114modelops/1948cohortaudits. Allrowshards terminal; failed66–78
stayunqualified. Executed79915ca only; workloadSDK/fullsourceinventory/physical
stores absentfromoriginalupload, finaldrain named-test scope. Full200/current
source/fullmatrix/24h and failedparent remainopen.
[Complete187-seed ledger](scale/current-tier3-block-stall-2026-10-05/).

### Full six-boundary combined500-child race qualification — 2026-10-05

Executed25b327c actualSDK nativePASS777.42s, allsix independentR3 creation/
results first-interior-last cases. Each actualparentSDK SIGKILL plus library
journal leader restart admitted;500children/result249500/501invocations-journals-
terminals/exactprefix/higher successor epoch. All500result signals prepared
before SDK collection, no earlyterminal/result; original5m case/30s cut retained.
Actualparent/sixchildSDKs/producerGit/667Gitinputs/3287selectedexternal inputs/
R3fault sequence metadata and16750completearchive members/threeparts verify.
Ten-control guard requires/accepts allsix passes. Original stores remain closed;
final integrity/results/prefix named-test scope. No independent copied integrity,
physicaldrain, every500position, NATSprocessSIGKILL, fullmatrix/24h or priorfailed
parent promotion. Production runtime behavior/timing unchanged.
[Complete successful proof and preserved failures](scale/fanout-combined-boundaries-2026-10-05/).

### Combined fanout CI dispatch schema correction — 2026-10-05

Dispatch at140c5eb rejected by GitHub before a run: job-level env cannot use
runner.temp. Use a fresh absolute /tmp root keyed by GitHub runid/attempt instead,
and shallow HEAD checkout (exact selected HEAD Git bytes remain available;
historical artifact objects are unnecessary to the producer). Corrected workflow
will be dispatched after push. No CI native result or qualification claimed by
registration/dispatch; local25b327c six-boundary qualification remains accepted.

### Sparse combined fanout CI source checkout — 2026-10-05

The repository has18GiB Git history and9.7GiB scale artifacts locally. The new
manual CI needs every tracked Go source (including four historical docs Go files)
plus modules/scripts/integration fixtures, not proof archives. Checkout uses
blob:none and non-cone **/*.go plus explicit required paths. Producer still
verifies/captures allselected HEAD Go inputs and exact producer Git bytes; sparse
status must remain clean. Native actual500/six/race/5m/30s requirements unchanged.
CI execution pending; no environment qualification from this preparation.

### Accepted hosted combined fanout launch — 2026-10-05

The corrected manual seed1 dispatch at executed7a4d739 was accepted as
run37333174296/job111841179439, observed queued. Original HTTP422/schema rejection,
corrected request/response, exact workflow and native source-bound metadata are
retained. Primary checkout@v4 source separately applies sparse patterns with the
blob filter; actual checkout and terminal native proof remain pending. This is
launch evidence only; local six-boundary25b327c qualification remains accepted,
with full current-source matrices and actual24h still open.
[Complete launch proof](scale/fanout-combined-boundaries-2026-10-05/ci-launch/).

### Independent full six-case fanout retained audits — 2026-10-05

Each successful25b327c cluster was independently reopened only as a fresh
byte-identical copy. Actual review SDK/b815450helper/selectedoriginalsource inputs
are verified;501invocations/journals/terminals match native reports,500 exactchild
results plusparent249500 readthrough allthree clients. Fullcopiedparent prefixes
match pre-SIGKILL records with higher successor epochs. Audits0.214–2.718s,
whole2.270–6.876s underoriginal20s. Originalsunchanged; completecopiedmedia/source/
executable/process/review archives readback verified. All64durables observed,
queuesnonzero throughallthree: creation500/493/500, eachresults501. Physicaldrain
is unqualified; no originalreopen/historicalcause/fullmatrix/24h promotion.
Initial helper compile failure preserved separately; no native SDK ran there.
[Complete six copied audits](scale/fanout-combined-boundaries-2026-10-05/copied-audits/).

### Combined fanout physical-drain profile prepared — 2026-10-05

Independent copies of the accepted25b327c six-case clusters retain493–501 run
messages despite501terminal workflows. The fixture canceled its workers as soon
as the parent result was visible. A new opt-in `--physical-drain` producer profile
starts production workers on all64partitions after joining the original workers;
no purge/manual acknowledgment. Within30s and the original5mcase it requires
zero queue messages/currentR3 and all64 exact durable names with zero pending/
ackpending through allthree clients, joins all64loops, then repeats the witness.
Rawstream/durable metadata are retained; bounded failures retain last observations.
Prefix/higherepoch/integrity/result checks follow drain; original30scut unchanged.
Acceptance optionally requires the drain witness after bothfaults/beforeprefix;
12guard controls pass. Hosted workflow adopts the profile; fullnative run and
independentcopied post-drain audit pending, no new physicaldrain qualification.

### Full physical-drain fanout native launch verified — 2026-10-05

Fullsix-case seed1 raceproducer at3413731 started actualSDK405574; actual/proc
SDK/exactproducer/668Gitinputs/3287external inputs independently verified. Opt-in
physicaldrain profile uses production64partition deliveries/confirmedallpeerqueue/
durable state and joins inside30s/original5m. Firstcreationcut admittedactual
parentSIGKILL/libraryjournal restart; terminalcase/matrix/copy review pending.
Twelveguardcontrols pass, racecompile/optinskip1.022s. Hostedolder7a4d739 run
nowinprogress, checkout/setup/controls passed; nativeproducerstep running.
No new physicaldrain/final-source/fullmatrix/24h qualification claimed.
[Complete immutable launch proof](scale/fanout-combined-boundaries-2026-10-05/physical-drain-launch/).

### First physical-drain case assertion failed; full run pending — 2026-10-05

Creationfirst3413731 recoveredworkflows but new30s drainassertion failed with
peer0queue138messages/64consumers atdeadline. Rawlaststream metadata and native
log slice preserved separately from immutable launch archive. ActualSDK405574
remainslive executinginterior; fullnative verdict pending. Diagnose remaining
messageidentities/operations from freshcopies afterterminal before any changed
profile; no unchangedrerun, originaltimeout extension or servercause claim.
[Live first-case observation](scale/fanout-combined-boundaries-2026-10-05/physical-drain-launch/first-case-observation.json).

### Hosted fullsix combined500 fanout accepted — 2026-10-05

Executed7a4d739 native racePASS257.12s; run37333174296/job111841179439/artifact
11355718693 source-bound terminalsuccess. ExactuploadedZIP/GitHubdigest/all16758
filemembers/threeparts readbackverified; noduplicate expanded archive. ActualSDK/
cleanGit/race/childSDKcaptures/exactproducer/667Gitinputs/3287selectedexternal
inputs verified, originalchecker acceptance regenerated equal. Six actualparent
SIGKILL/libraryR3restarts/currentreplicas/prefix/higher epoch/500childresults/
parent249500/501invocations-journals-terminals verified. Hostedproc provenance
through exactproducer/nativejoblog; finalintegrity/result/prefix named-testscope,
noindependentcopy/physicaldrain/fullmatrix/24h orhistoricalfailure promotion.
[Complete hosted proof](scale/fanout-combined-boundaries-2026-10-05/hosted-37333174296/).

### Accepted fanout terminal-parent queue identified — 2026-10-05

Freshcopies ofaccepted25b327c creationfirst show all500rawWF_RUNmessages are
parent.large-fanout onwf.run.52; parentCompleted/all500childterminal/64durables,
onlyWF_P_52nonzero499pending1ackpending. Stablecount/tail/fullcensus,1.023swhole
underoriginal20s. ActualSDK/helperGit/selectedsource inputs/2093originalfiles
unchanged verified; completecopy/source/executable/process/diagnosis archived.
Thislocalizes residualterminalwakeups; itdoesnotprove later3413731 timeoutcause
orqualifyphysicaldrain. Newprofile fullterminalverdict stillpending.
[Complete queue diagnosis](scale/fanout-combined-boundaries-2026-10-05/accepted-queue-diagnosis/).

### Complete diagnostic physical-drain matrix failed — 2026-10-05

Executed3413731 raceFAIL963.02s; allsixfull500parentSIGKILL/libraryR3restarts
admitted. Onlyresults/interior passes64joinedworkers/allthreezeroqueues/64durables/
exactprefix/higher epoch/501retainedfinalproof; named-testscope. Five extra30s
checks fail; fourlastsnapshots138/121/80/113messages, creationinterior readdeadline.
ActualSDK/exactproducer/668Gitinputs/3287externalinputs/sixchildSDKs verified;
completeclosedoriginalmedia/source/SDK/fault/failure/review proofreadback. Original
5mcase/30scut unchanged; no physicaldrainmatrix/fullrelease qualification. Strict
originalchecker rejectsfailedmatrix. Diagnosefreshcopies afterpreservation; first
copylaunch stopped atopen-file safety before SDK/root creation whilearchiveractive.
No unchangedrerun orservercause attribution.
[Complete failed profile](scale/fanout-combined-boundaries-2026-10-05/physical-drain-failed/).

### New failed-drain copy observation deadline retained — 2026-10-05

Actualread-onlySDK onfresh3413731 creationfirst copies failed withbarecontext
deadline; noqueue census orreport, stagecauseunconfirmed. ActualSDK/helperGit/
1655selectedinputs/originalfilesunchanged verified, completeclosedcopystore/
source/executable/process/error archive readback. Helperprepared withstage
logging and19sboundedcomplete stack insideunchanged20s, beforeanotherfreshcopy.
No unchangedrerun/productionchange/budgetextension/servercause attribution.
[Complete failed copied observation](scale/fanout-combined-boundaries-2026-10-05/failed-drain-queue-diagnosis/).

### Instrumented failed-drain copied queue identified — 2026-10-05

Changedb3268e7helper fresh3413731 creationfirst copies pass4.331sread/4.351s
nativewhole underunchanged20s. Parent2504records27ms/500childtails300ms/consumer
census4.006s/rawqueue10ms;19spredeadline timer canceled, no stackclaim. All137
remainingmessages terminalparent onpartition52, stablecount/tail;64durables/
only52nonzero136pending1ackpending. ParentCompleted249500/all500Completedchild
exact2×indexresults/preservedthree-recordprefix/higher epoch verified. ActualSDK/
helperGit/1655inputs/2090originalfiles unchanged verified, completecopyproofarchive
readback. Original livefailure138beforejoin vsclosed137. Earlierbarecopydeadline
remainsfailed; no originaloperationtiming/causefix/drain/fullmatrix/24h claim.
[Complete changed observation](scale/fanout-combined-boundaries-2026-10-05/failed-drain-queue-trace/).

### Fanout drain target aligned to original case deadline — 2026-10-05

The separately added30-second terminal-backlog drain bound in3413731 was a
diagnostic target; the suppliedfanout proof uses its existingfive-minute case,
and30-second cut admission is a separate unchanged limit. Nextphysicaldrain
qualification inherits that originalcase deadline for workers/reads/joins and
retainsallthree currentR3 zeroqueues/exact64zeropendingdurables, committedprefix,
higher epoch/501integrity/exactresults. No separate recovery-p99 claim is derived
from terminal-backlog drainage. The same drain witness now recordscase_deadline.
Earlierstrict30s profile remainsfailed/preserved; earliercopieddeadline remains
failed. Fullchanged nativeprofile pending. No productionruntimechange.

### Full six fanout boundaries with physical drain accepted — 2026-10-05

Executeddc8422a racePASS579.62s. Allsixfull500creation/result actualparentSIGKILL/
libraryR3journal restarts admitcuts; exactprefix/higher epoch/parent249500/501final
retained report. Each all64productionloops joined; allthreecurrentR3 zeroqueues/
exact64zero pending/ackdurables, allrawmetadata timestampsbeforeoriginalcase
five-minute deadline. Original30scut unchanged. ActualSDK/sixchildSDKs/exactproducer/
668Gitinputs/3287externalinputs verified;12-control allsixphysicaldrain guardaccepts.
Complete16760archive members/threeparts readbackverified. Drain10.047–25.611s;
olderdiagnostic30s failure remainsfailed, no causefixattribution. Originalstores
closed; independentfresh-copy audits pending. Noall500positions/serverprocess
SIGKILL/current-source fullmatrices/24h qualification.
[Complete successful physical-drain proof](scale/fanout-combined-boundaries-2026-10-05/case-deadline-drain/).

### Independent full six-case fanout integrity and drain audits — 2026-10-05

Each successfuldc8422a cluster was independently audited onlyas a freshbyte-
identical copy. Each501invocations/journals/terminals matchsnative report;500unique
childrequests/allthreeclient exact2×indexresults/parent249500, preservedactual
pre-SIGKILL prefixes/higher epochs. Allthreequeueszero/exact64durableszero pending/
ack. Audit2.358–2.612s/whole6.100–6.869s underoriginal20s excludingstartup/readiness.
ActualSDK/proc/1687selectedinputs/helperGit/original2093/2091/2090/2156/2157/2160
filesunchanged verified; all23907archive members/elevenparts readback. No workers/
manualacks/originalreopen/historicalcause/all500positions/fullmatrix/final-source/
24h promotion. This closesindependentcopy integrity/result/prefix/drain scope for
sixcombinedboundaries. [Complete copied audits](scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/).

### Retained Tier2 process-row producer — 2026-10-05

`scripts/run-tier2-retained-row.py` and the manual `tier2-retained-row`
workflow prepare retained execution for journal-leader, consumer-leader,
all-server-kill and server-partition rows. The default remains the original
normal ten-minute Tier2 profile; optional race and 35-second smoke profiles
are explicitly recorded. Smoke does not qualify the ten-minute gate.

Evidence includes the actual live SDK executable identity, clean committed
source and selected dependency bytes before/after, native output and duration
acceptance, original closed process stores and point observations of owned NATS
server executables read through `/proc`. Polling can miss short-lived processes;
it does not establish exhaustive executable coverage. Independent fault/history
and copied-store review remains necessary. The full thirteen-row/200-seed
requirement is unchanged. Initial runner verification is pending.

### Retained Tier2 smoke and duration conversion correction — 2026-10-05

The new journal-row producer executed normal seed1/original35s smoke at584aac5:
native PASS46.69s,196 terminal invocations, one admitted journal-leader restart.
ActualSDK and four observed server processes,668 selected Git and3287 selected
external inputs independently verified. Producer acceptance failed because
conversion omitted elapsed fields; that failed result is preserved. Independent
`test2json -t` conversion of the same native log passes the unchanged checker.
The producer now requests `-t`; generated timestamps are conversion observations.
[Complete smoke evidence](scale/tier2-retained-row-2026-10-05/journal-smoke/).
No ten-minute, independent copied-store or full-matrix qualification is claimed.

### Copied Tier2 matrix review preparation — 2026-10-05

`scripts/matrix-retained-review.go.txt` prepares independent review of fresh
copies of closed process-matrix stores. It rechecks all three recorded client
history models with the existing30s checker budgets, starts the copied R3
process identities, observes existing assets without provisioning, and audits
full retained integrity/counts plus all-three-client physical queue and64durable
pending/ack state within the original20s audit deadline. No workers/manualACK
or original-store reopen. Helper compilation and copied execution remain pending;
this does not replace native fault or full-matrix qualification.

### Copied matrix smoke succeeds; observer task-thread gap identified — 2026-10-05

Fresh copies of closed journal-smoke process stores passed full196/2156 retained
integrity, independently rechecked three history models and all-three-client
queue/64durable drain. Audit198.5ms/full read4.212s under20s. Originalfiles unchanged.
The helper's actualSDK was observed, but zero server executable observations were
captured. Inspection identifies a coverage gap: Linux children files belong to
individual task threads, and v1 only traverses the main task. Go may launch server
processes from another thread. Existing observed executable records remain valid;
exhaustive coverage was never claimed. V2 traverses every task's children. Current
live partition producer's source stays unchanged; changed copied observation will
use v2. Full independent server provenance review remains pending.

### All-task-thread copied process review accepted — 2026-10-05

Changed observer on a freshcopy captures allthree actual NATS process executable
bytes, each hash matching originalsmoke captures. ActualSDK460234 /helpera1abbaf,
1695selected inputs bound,2154originalfilesunchanged. Allthree histories rechecked,
full196/2156 integrity/drain;304.296ms audit/4.327874s reads underoriginal20s.
Complete original zero-observation copy and changed copiedproof retained separately.
[First copied review](scale/tier2-retained-row-2026-10-05/copied-smoke-v1/);
[changed observer qualification](scale/tier2-retained-row-2026-10-05/copied-smoke-v2/).
Current native partition row remains live at ec60e83; no ten-minute verdict yet.

### Original retained Tier2 partition seed2 accepted — 2026-10-05

Executedec60e83 normal seed2/tenminutes, nativePASS651.13s and producer acceptance0.
71batches/1988terminalinvocations/21931entries/19admittedpartitions;
aggregate terminalp99=13.324673134s, allsix terminal/progress cells below30s.
ActualSDK/three observed servers/exactproducer/668Git+3287external inputs bound;
completeclosed originalstore/executable/source proof retained. Reviewer's initial
10m0s-vs10m formatting error preserved; existing durationparser confirms native
schedule/count/cells. [Complete native proof](scale/tier2-retained-row-2026-10-05/partition-seed2-ten-minute/).
Independentcopied review follows; full13x200 gate remains. All-task-thread
observation is now adopted for future portableproducers after this run closed.

### Independent copied ten-minute partition audit accepted — 2026-10-05

Freshcopies of closed ec60e83 partition seed2 after nativepreservation pass all
three historymodels, full1988/21931 retainedintegrity and allthreeclient queues/
64durables zero pending/ack. Original2314files unchanged. ActualSDK467006,
helper121b061/1695selectedinputs andthreeactualserverexecutables verified.
Audit414.343ms/read4.424010s withinoriginal20s; completecopiedproof readback.
[Complete independent audit](scale/tier2-retained-row-2026-10-05/partition-seed2-copied-audit/).
Only original ten-minute seed2 is qualified; full13x200/current-source remainsopen.

### Retained block-fault filesystem media preparation — 2026-10-05

Retained process-matrix campaigns now ask `BlockDisk.RetainMediaOnClose` to keep
its private backing image after servers join and all owned devices are resumed,
unmounted, removed and detached. Ordinary fixtures keep deleting their private
images. The retained image path is logged for closed-copy review; a symlink to an
unmounted store is not filesystem evidence. A real Linux test prepares a file,
closes the fixture, copies the image, mounts only that copy read-only with noload,
checks the file and verifies both image hashes unchanged. Test execution and
actual retained block-row qualification remain pending.

### Real retained-media control accepted; portable block row prepared — 2026-10-05

At executede453ed6, actual raceSDK472174 passed real loop/device-mapper ordinary
stall/cleanup5.55s and closed-image retention/copied-read-only check3.78s.
The copied file and unchanged original/copy image SHA verified before test-temp
cleanup; this is fixture correctness evidence, not retained native block-row
qualification. The portable retained runner/workflow now select blockdisk,
verify prerequisites and require a passing row to retain exactly one512MiB
closed backing image with recorded hash. Original durations/audit/fault/drain
bounds unchanged; real row execution and independent copied filesystem audit
remain pending.

### Retained block-row smoke and copied raw-image audit accepted — 2026-10-05

Executed2774fd2 normal/original35s seed1 passes45.13s:224terminals/2474entries,
one real5s stall/alllatencycells<30s. ActualSDK/allthreeobservedserverexecutables/
668Git+3287externalinputs reviewed. Exactlyone512MiB originalimage retained after
ownedmount/device cleanup; fullclosedproof readback. A fresh rawimagecopy mounts
only undercopiednode-2, alongsidecopiesofotherclosednodes. Allthreehistorymodels,
224/2474integrity andallthreeclient queue/64durable drain pass in4.217606s under20s.
ActualSDK/1695inputs/threeobservedserversverified, original1397files/rawhash unchanged,
copiedmountdetached andclosedcopyproof readback. No originalimage mount orNATS
store reopen; no workers/manualACK/provisioning. [Nativeproof](scale/retained-block-media-2026-10-05/block-row-smoke/);
[copiedrawaudit](scale/retained-block-media-2026-10-05/block-row-copied-audit/).
Ten-minute blockrow/full13x200gate remainpending.

### Retained ordinary worker-process executable observation prepared — 2026-10-05

`scripts/matrix_worker_observer.py` prepares periodic owned worker-generation
captures for the Tier2 worker-kill/pause/isolation rows. Ancestry includes all Go
task threads, exact child test selection and fixture identity/root fields are
checked, and actual live `/proc` executable bytes are retained with PID/start
rechecks. Other environment values are not recorded. One owned identity and six
foreign/malformed controls pass; real worker-row execution and producer integration
remain pending. Polling cannot establish exhaustive generation coverage. Current
live disk-row producer and its captured source files stay unchanged.

### Portable copied Tier2 review command prepared — 2026-10-05

`scripts/run-tier2-copied-audit.py --donor ABSOLUTE_CLOSED_ROOT --root FRESH_ROOT`
prepares independent copies, binds helper/producer/observer and selected source,
checks counts from the native retained report and rechecks all histories plus
integrity/drain using the existing helper. `--mount-copied-block-image` is required
for block donors and mounts only matching copied raw media; cleanup stops owned
processes and detaches that mount. Dependency comparison uses captured module/
toolchain relative paths, allowing matching source across host cache locations.
No original store is reopened; failed/native-live donors are rejected. Native and
full-matrix verdicts remain separate. CLI/import checks pass; real command
execution remains pending. Current live disk producer source stays unchanged.

### Original retained ten-minute block-disk seed1 accepted — 2026-10-05

Executedf57da4d normal/originaltenminutes passes657.37s:114batches/3192terminals/
35153entries,19verified5sdevice-mapper stalls andalllatencycells<30s,aggregate
terminalp99=5.026275634s. ActualSDK/allthreeobservedservers/668Git+3287external
inputs reviewed. Closed512MiB originalimage retained after cleanup; full5512member/
fourpart proofreadback. [Complete native proof](scale/retained-block-media-2026-10-05/ten-minute-native/).
Independentcopiedrawaudit follows. Originalgate budgets unchanged; full13x200open.

### Retained ordinary worker and fanout-restart rows prepared — 2026-10-05

After the live block SDK completed, the portable producer/workflow now select
worker-kill, worker-pause, worker-reply-isolation and fanout-restart in addition
to the prior five rows. Worker and server observers are each bound to exact Git
bytes before/after; owned worker generations retain actual executable captures
and identity fields. Native fault/audit/history/latency/drain gates and durations
remain unchanged; observation errors retain failed evidence. Clock/upgrade rows
still need their generated overlay/legacy executable retention paths. CLI/import
checks pass; real new worker-row verification remains pending. Full13x200open.

### Portable copied ten-minute block audit and retained worker smoke accepted — 2026-10-05

Newrepository copiedcommand at6612ffd passes full3192/35153integrity, allthree
historymodels andallthreequeue/64durable drain in4.524316s underoriginal20s.
ActualSDK/1695inputs/threeobservedservers verified; original1525files/rawimage
unchanged, copiedmountdetached andfullclosedproof readback.
[Complete portable audit](scale/retained-block-media-2026-10-05/ten-minute-copied-audit/).
Extendedworker producer atede2fbd passes original35ssmoke54.47s/140terminals/
1550entries/sixSIGKILLs/twoactivefaults. Nineactualworker generationcaptures include
all sixfaulttargets, eachsamebytesasparentSDK; allPIDs gone. Fullsource/closed
stores/executables/review readback. [Complete worker smoke](scale/tier2-retained-row-2026-10-05/worker-smoke/).
Worker ten-minute/full13x200 gates remainopen; copiedordinaryworker-path review
follows separately.

### Portable ordinary copied review and first diagnostic timer restart — 2026-10-05

Repositorycopiedcommand atd893389 verifiesordinary worker-smoke path: allthree
histories/140terminals/1550entries/allthreequeue64durable drain in4.208218s under20s,
actualSDK/1695inputs/threeobservedservers andoriginal2091filesunchanged. Fullclosed
proofreadback. [Complete ordinary copied proof](scale/tier2-retained-row-2026-10-05/worker-smoke-copied-audit/).
Live diagnosticmillion candidate at68d69c04 records firstplanned allthreeSIGKILL
PIDs101317/101318/101319, after333341receipts, healed16.875646s; deliverycontinues.
Actualprogram101283 checkedlive, savedrunningreport342992receipts. No finaldrain
orserveradoption qualification. [Point restart evidence](scale/million-candidate-24h-2026-10-05/first-restart/).
Actual24h journalSDK162125 also live; checkpoint2810/78680terminals accepted.
Fullmatrix/24h/originalmillion terminalgates remainpending.

### Complete retained Tier2 profile selection prepared — 2026-10-05

`scripts/tier2_retained_profiles.py` prepares allthirteen originalrow mappings,
original18m/clock20m SDKtimeouts andsupplied NATS2.11.17 executable retention.
Mappings match the originalcampaign planner; identity controls distinguish a main
module from a dependency/unrelated module. Actualprocess executable observations
remain separate from suppliedbytes/buildinfo. Integration into producer, generated
worker-clock overlay retention andreal clock/upgrade verification remainpending.
Current live worker SDK andits source captures are unchanged.

### Generated test-main cache lifetime correction — 2026-10-05

The original75bcf76 worker ten-minute native row passes632.43s/1708terminals/
18918entries/119confirmedSIGKILLs/31activefaults/p99=15.033519329s; all122worker
captures includeall119targets. Its producer after-check fails because the selected
external inventory included a Go-generated test-main cache file, removed by
explicit build-cache cleanup after nativebuild. Failure preserved; no native rerun.
All668Git and3286durableexternal inputs remain unchanged; generatedfile's captured
bytes match initialSHA. Separate duration/source/fault review accepts native
results without asserting the missing cachepath survived. Futureproducer separates
retained generatedinputs from durableexternal before/after checks. Copiedcommand
`--reviewed-donor` uses this explicit separate review path; originalproducer failure
is retained. Real correctedproducer/copiedpath verification remainspending.

### Allthirteen retained Tier2 rows integrated — 2026-10-05

Portableproducer/workflow now selectallthirteen originalrows, retaining original
clock20m/other18m SDKtimeouts. Upgrade requires suppliedcaptured NATS2.11.17
executable; workflow builds pinnedlegacyversion andliveobservers recordactual
old/new processbytes. Worker-clock generatedGo overlays/skewedSDK builds now live
underretainedroot; server-clock overlay/serverbuilds already reside in retained
case roots. Profilemodulebytes boundbefore/after. Clock/upgrade realexecution and
updated generated-input classification verification remainpending. No fullmatrix
orolderfailure causefix qualification. Ordinaryruntime source behavior unchanged.

### Independent copied ten-minute worker audit accepted — 2026-10-05

The explicit reviewed-donor path at79fc537 completes the original75bcf76 worker
seed1 copied audit: allthreehistorymodels,1708invocations/journals/terminals,
18918entries andallthreeclient/64durable queue drain. Fullreads401.033ms under
original20s;2281originalfiles unchanged. ActualSDK522835 andthreeactualNATS
executables observed;1695selectedinputs bound. Complete4001archive members and
bothparts readback. Originalproducer generated-cache failure stays preserved;
this is separate copied integrity/history/drain proof, not an originalproducerpass
orfullmatrix qualification. [Proof](scale/tier2-retained-row-2026-10-05/worker-ten-minute-copied-audit/).

### Retained worker-clock smoke and generated-input handling verified — 2026-10-05

Executed828db48 native35s worker-clock smoke PASS266.00s including shiftedSDK
builds;196terminals/2171entries/p99=5.088019049s. Actual+5s/-5s samples atinitial
and30s cuts, allsix terminal/progress cells<30s. Independent source/actualprocess
review verifies668Git/3286durableexternal/onegeneratedinput andbothretained
clockoverlays withmatchingactualshiftedSDKbytes. Producer/checker exit0,three
workers/threeNATSobservations/closedPIDs. Fullproof readback. Smokeonly;
copiedaudit,ten-minuteclock andfullmatrix remainseparate.
[Proof](scale/tier2-retained-row-2026-10-05/workerclock-smoke/).

### Hosted retained ten-minute partition seed3 accepted — 2026-10-05

Run37347246576/job111888806507 executed a517d2e normalPASS625.04s;
2044terminals/22537entries/19partitions/p99=13.56736368s. Producer andduration
checker exit0. Independent source/fault/cell/duration review passes;668Git/3287
external inputs andactualSDK/threeNATS binaries verified. ExactoriginalZIP6295
members/fourparts readback; SHA256matchesGitHub digest. Copied-store audit and
full13×200/final-source gates remainseparate. [Proof](scale/tier2-retained-row-2026-10-05/hosted-partition-seed3/).

### Worker-clock copied smoke audit and hosted source compatibility — 2026-10-05

Portablecopiedclock audit at0b8ee59 passes full196/2171integrity/allthreehistory
models/threeclient64durable drain in312.719ms under20s;2124originalfiles unchanged.
Independent reviewer/source/SDK/threeNATS verified;4034archive members/twoparts
readback. [Proof](scale/tier2-retained-row-2026-10-05/workerclock-smoke-copied-audit/).
Originalten-minuteclock seed1 nowruns at0b8ee59,actualSDK539522.

Hostedpartition copiedpreparation usingcurrent inputs fails beforeSDKbuild/store
opening: laterblock-media retention changedtestcluster/block_disk_linux.go from
hosteddonora517d2e. Originalfiles verifiedunchanged; failure retained. Isolated
sparsehelpercheckout605a78a restoresdonorproduction inputs while retaining the
portablehelper; changedpreparation runs inafreshroot. No runtime failure or
copiedaudit passclaimed. [Preparation record](scale/tier2-retained-row-2026-10-05/hosted-partition-seed3/copied-preparation/).

### Independent hosted partition seed3 copied audit accepted — 2026-10-05

Bound-source helper605a78a completes hosted a517d2e donor review: full2044
invocations/journals/terminals/22537entries,allthreehistorymodels andthreeclient/
64durable drain in1.046703s underoriginal20s. All2314originalfiles unchanged,
1695selectedinputs/actualSDK542506/threeactualNATS verified. Complete4036archive
members/twoparts readback. Initialpreparation source mismatch remainspreserved;
no originalstore reopen ornative rerun. [Proof](scale/tier2-retained-row-2026-10-05/hosted-partition-seed3-copied-audit/).

### Retained upgrade smoke queue-drain failure preserved — 2026-10-05

ActualSDK545168 at123bc4b normal35s upgrade seed1 fails86.82s. Full168terminals/
1852entries andoneupgrade admitted,allterminal/progress cells<30s; finaloriginal
30s queue drain leavesone rawmessage. Identity/causeunconfirmed. Independent
668Git/3286external/onegenerated/actualSDK/threeinitiallegacy+onecurrentNATS
verification passes; suppliedlegacybytes matchactual2.11.17processes. Complete
6173archive members/fourparts readback. No unchangednative rerun ororiginalstore
reopen. Newread-onlycopiedqueuehelper retainsresidualmessage/journal/state/lease/
consumercensus; diagnosticexecutionpending. [Failed proof](scale/tier2-retained-row-2026-10-05/upgrade-smoke-failed/).

### Upgrade queue discrepancy localized on copied stores — 2026-10-05

Originalfailedsmoke rawartifact identifiesseq494/wf.run.45,signal-wakeup:193,
keymatrixchild.c-8e42390a21f5c779a68ee1399bb48a2e. All64nativeconsumers reportzero
pending/ackpending despiterawstreammessage1. Freshcopies reopened undercurrentR3
findzeroqueue/64durableszero withoutworkers/ACKs/provisioning;2175originalfiles
unchanged,1655inputs/actualSDK547767 verified. Complete3987archive members/twoparts
readback. Cause/timeofdisappearance unconfirmed; nomixed-versionlifecycle
reproduction orrecoveryqualification. Helper nowacceptsexplicitTYPE.ID for
journal/state/lease inspection evenifresidualqueueisabsent.
[Copied diagnosis](scale/tier2-retained-row-2026-10-05/upgrade-smoke-copied-queue-diagnosis/).

### Original ten-minute worker-clock seed1 and targeted upgrade diagnosis — 2026-10-05

Retainedworkerclockat0b8ee59 normalPASS638.48s/103batches/2884terminals/
31877entries/19verified+5s/-5s events/p99=5.152018311s. Producer/checker/source/
actualSDK/overlays/workerandNATSbytes independentlyverified;6337archive members/
ninepartsreadback. Originalten-minute seed1qualifies; copiedaudit/fullmatrix open.
[Nativeproof](scale/tier2-retained-row-2026-10-05/workerclock-ten-minute-native/).

Targetedfreshcopyofthefailed123bc4b upgrade provesresidualwakeup'schild consumed
signal193/completedstep193,has13contiguousjournalentries/Completedresult7,
matchingsnapshotinv_seq154 andnolease. All2175originalfilesunchanged; no workers/
ACKs/provisioning. Nativequeue1/64durableszero inconsistency remainsunconfirmed;
currentR3copiedrestartisnotoriginalmixed-versionlifecycle reproduction.
[Targetedproof](scale/tier2-retained-row-2026-10-05/upgrade-smoke-copied-child-diagnosis/).

### Complete seven-hour failed soak preservation and copied clock acceptance — 2026-10-05

Actual24h aace912 fails25201.93s atcheckpoint3140/cutoff87920 underoriginal
20s/60s/threeattemptlimits. Independentcomplete9063originalarchivefiles/1374
sourceinputs/actualobservedSDK162125 verified. Outer25members/fifteenpartsreadback.
Attempts1/2read970142/970546journalentries beforestate-watch failure; attempt3
expiresduringjournalread. Countersnotacceptedreports; lastaccepted3130/87640/
966835. Originalstoresclosed,no unchangednative rerun orservercauseclaim.
[Completefailedproof](scale/watch-observed-journal-24h-2026-10-05/failed-3140/).

Independentcopiedworkerclockten-minute audit atf0f37c8 qualifies full2884/31877,
allthreehistorymodels andthreeclient64durable drain in4.431885s underoriginal20s;
2317originalfilesunchanged/1695inputs/actualSDK551527/threeNATS verified.
Complete4037members/threepartsreadback. Fullmatrix gate remainsopen.
[Copiedproof](scale/tier2-retained-row-2026-10-05/workerclock-ten-minute-copied-audit/).

### Concurrent retained-state audit candidate prepared — 2026-10-05

Checkpoint3140 attempt2 finishesjournal18:46:43.760318 andwatchcreation
18:46:43.861157 against18:46:44.013deadline: approximately265ms leftafterjournal,
152ms afterwatchcreation. Passing3130 attempt2 watchspendsapproximately1.366s.
Incompletewatchtherefore doesnotalone establishserverfailure. Explicitexperimental
concurrentstate APIs now overlap completefreshKVsnapshot withjournalreading once
invocationcohortisfixed. Originalsequentialdefaults/20s/60s/threeattempts andfull
validation remainunchanged. Snapshotiscancelled/joined onjournalfailure.
Nativecompaction/corruption/state racecomparisons pass65.995s; cancellation/initial
barrier racecontrols pass1.017s. Full87,920copiedcohort comparison isnext; noadoption
or24hqualification. [Controls](scale/concurrent-state-audit-2026-10-05/controls/).

### Complete87,920 copied-cohort concurrent state comparison accepted — 2026-10-05

Executed8947bba SDK556940 nativePASS55.02s. Original20s sequential/concurrent/
sequentialrecheck allpass8.106896/7.876084/7.555520s,identical87920invocations,
journals,terminals/969925entries. No speedup attribution. All3886originalstore
files unchanged andverifiedagainstoriginalarchive;669Gitinputs/fiveactualNATS
bytes/mounts/closedPID independentlyreviewed. Complete4419archive members/
sixteenpartsreadback. Quiescentcopiesonly,nofault/writers; candidateexperimental,
notoriginal24h qualification. [Proof](scale/concurrent-state-audit-2026-10-05/copied-87920-comparison/).
Fullcohort observedstatewatchleaderSIGKILL testprepared; original20sreport gate
unchanged. Compile/opt-inskip verified,actualfaultexecutionpending.

### Full87,920 concurrent copied-cohort state-leader fault accepted — 2026-10-05

Executed44c3f6e SDK560589 nativePASS23.24s,complete87920invocations/journals/
terminals/969925entries in9.830075s underoriginal20s. Actualstatewatchconsumer
leadernode3SIGKILL admitted with86144pendingentries; processstopped confirmed.
Firstwatch11685records/noinitialbarrier discarded; second88068records/complete
initialbarrier accepted. All3886originalstorefiles unchanged andindependently
verifiedagainstoriginalarchive;669Gitinputs/fiveactualNATS/closedPIDs verified.
Completeclosedproof archiving underway. No runtimewriters/original24h qualification
orconcurrentcandidate adoption. [Independentreview](scale/concurrent-state-audit-2026-10-05/copied-87920-state-leader-loss/independent-review.json).

### Concurrent reader journal/legacy controls and explicit live profile — 2026-10-05

Concurrentjournal consumerleaderloss/cancellation racecontrols pass47.536s,
full1500/6000 reportrecovery1.915392s/4039pendingatcut; cancellation128visits/
273.517ms/context.Canceled. Existinglegacy2.11.17 comparisonsincludeconcurrent
mode andpass48.448s. NinePythonprofile controls/Go routingrace1.017s pass.
Explicit--concurrent-state-retained-audit routescheckpoint andfinalaudits;
originalsequentialdefaults/20s/60s/threeattempts/sync/memory/gate arguments stay.
Fullcopiedstateleaderloss4421members/sixteenparts nowpreserved/readback.
Live10m journalfault/workloadverification isnext; no24h qualification/adoption.
[Controls](scale/concurrent-state-audit-2026-10-05/live-profile-controls/).

Closeddiagnosticcomparison copied-stores reclaimedonlyafter all3720files match
committedarchive,allactualPIDs gone andnoopenFDs. Originalstores/source/logs/
executables andcompletecommittedarchive retained; copycanbereconstructed.
Reclaimed1,964,510,320bytes. [Record](scale/verified-duplicate-archive-cleanup-2026-10-05/archived-comparison-copy/).

### Experimental concurrent reader live journal profile accepted — 2026-10-05

Executedbc02e68 normal seed1 PASS648.01s: continuous ten-minute workloads,
97batches/2716completedworkflows/29930entries/19confirmed journal leaderSIGKILLs.
Allsix terminal/progressp99cells below30s; nine completedcohort checkpoints and
final fullstate/drain pass with original20s/60s/threeattempt limits unchanged.
Independent source/archive/actualSDK/server/row review and preservation recorded
in [complete proof](scale/concurrent-state-audit-2026-10-05/live-journal-ten-minute/).
Concurrent reader remains explicitexperimental; this doesnot qualify24h capacity,
originalfailedsoak, fullmatrix or finalsource. No unchangednative rerun.

### Full400k R5 concurrent audit capacity profile prepared — 2026-10-05

A fresh synthetic quiet population gate prepares400,000 invocations, contiguous
12-entry completed journals (4.8M entries) and matching terminal state acrossfive
file-backed replicas. Eight independent publishers retain13 futures each, check
every ACK and preserve per-invocation ordering; setup deadline is separatefrom
unchanged20s audit attempts. Sequential/concurrent/sequentialrecheck verdicts,
complete reports, allocation andGC measurements persist beforeassertions.
Concurrent mustreturn exact fullreport within20s. This is capacity preparation,
not a live24h workload/fault qualification orcandidateadoption. Compile/opt-in
skip passes; nativepopulation execution pending. Closed earlierstateleader
copy reclaimed onlyafter3722files verifiedagainstcommittedcompletearchive,
allobserved processesgone/noopenFDs; originalfailedsoakstores remainintact.

### Full400k capacity preparation deadline failure preserved — 2026-10-05

Originalfb0cc9e fails35.96s in provisioning beforepopulation/audit. ActualSDK591181,
five actualserverobservations/selectedGitinputs/closedprocesses verified;
complete727-member archive readback/twoparts preserved. This isnot a400kcapacity
verdict. Changedpreparation adoptsbounded3s provision callswithin4m; population
setup90m and original20s audit limits remainseparate/unchanged. Compile/opt-inskip
passes. No unchangednative rerun orservercause claim.
[Originalfailure](scale/concurrent-state-audit-2026-10-05/capacity-preparation-failed/).

### Full400k quiet R5 audit capacity failure — 2026-10-05

Changedpreparation a124b69 populates400k invocations /4.8M contiguousentries/
400k matching terminalstates, allpublishACKs checked. NativeFAIL276.09s:
sequential/concurrent/sequentialrecheck eachhits original20sdeadline. Concurrent
allocated4.76GB/39GCcycles underGOMEMLIMIT2GiB; partialreportsnotfullcounts.
Noquietcapacitypass, actual24h/faultqualification, candidateadoption orservercause
claim. ActualSDK595173/fiveNATS/selectedGitbeforeafter/processclosure independently
reviewed; complete1752-member/24-part proof readbackverified. Measurephase/CPU/allocation
onverifiedcopies beforeanother24hlaunch. [Capacityfailure](scale/concurrent-state-audit-2026-10-05/capacity-400k/).

### Full400k copied capacity phase/CPU profiling prepared — 2026-10-05

A separate copied-store diagnostic records invocation/journal scan and visitor
elapsed time, delivered records/bytes, CPU phase labels and sampled allocations
for the same concurrent reader under its original20s deadline. Restoredexisting
R5 file streams mustretain exact400k/4.8M/400k sourcecounts; noprovisioning or
workers. Diagnostic testcompletion doesnot turnan individualfailed auditinto a
capacitypass. Compile/opt-inskip passes; nativeprofiling pending. Copies require
independent canonicalarchive/originalbyte verification beforeopening; originals
remainclosed. Verifiedpushed worktreearchive copies reclaimed4.019GB forheadroom;
canonicalGit/originalstores retained, exactparts/blobs/FDchecks recorded.

### Copied400k deadline localized during journal scanning — 2026-10-05

Executed64ece3e diagnosticPASS35.37s; individualaudit failsoriginal20sdeadline.
Invocation400k scan2.860823s, thenjournal2,610,401/4.8M records in17.136284s;
visitor6.343280s withinjournalphase. Measuredallocation3.7566GB/62GCcycles.
CPU18.17s total: labelsWF_JRN8.82s/WF_INV1.13s excludeunlabelledclientgoroutines.
Sampledallocation dominatedbyNATSprocessMsg/metadata/timers ratherthan journal
encoding. Copies boundtocanonicalarchive; originalsunchanged, source/actualSDK/
servers/closedprocess review retained. Profilinginstrumentation adds overhead;
no clean400kpass/defaultadoption/24h qualification orservercause attribution.
[PhaseandCPUevidence](scale/concurrent-state-audit-2026-10-05/copied-400k-phase-profile/).

### Bounded iterator context option reuse — 2026-10-05

The profile attributes59MiB of sampled allocation toNextContext. The byteiterator
now constructs one immutable context option per4096-record window instead ofper
record. PinnedSDK implementation onlyassigns the captured context tolocalnextOpts;
same context/deadline/cancellation remain. Existing racecancellation/error/heartbeat/
prefetch/replay controls pass1.019s. This isasmallallocation reduction, not a400k
capacityfix ortransport/metadata/timer optimization. No fullcapacitypass claimed.

### Experimental compact audit metadata reader prepared — 2026-10-05

An explicitscanner candidate avoids allocating SDKmetadata/token slices for
ordinarypinned v1/v2 ACK replies, returningstream/sequence/timestampbyvalue.
Unsupportedsyntax delegates toSDK; nonSDK wrappers retain Metadata overrides.
Defaults/publicAPIs remainSDK-based. Timestamp visitorcontract preserved after
initialcompilecheck caughtomission; failedcompilelog retained. Differential20010
subjects/edgecases againstpinnedSDK parser andzeroallocation checks pass; parser
benchmark SDK252.1ns/304B/2alloc versuscandidate155.9ns/0B/0alloc. This isnot a
fullaudit speedup or400kpass. Existingrace controls pass; sharednative compaction/
corruption/state comparisons nowinclude candidate. An explicitcopiedprofile flag
selects candidate withsame20s budget. Realnative controls/capacity remainpending.

### Compact metadata current native race contracts accepted — 2026-10-05

Executed2c4995e actualSDK616708/race/current2.15 libraryR3:
compaction/cohort/freshcorruption PASS56.09s, journalcorruption13.87s,
statevalues11.89s, native64delivery SDKcoordinates/timestamps/zeroalloc contract
4.39s. Sourcebeforeafter/actualSDK/race/VCS independentlyreviewed; completeclosed
proof retained. Parserbenchmarknot400kcapacity qualification; defaultsremainSDK.
[Nativeproof](scale/compact-audit-metadata-2026-10-05/native-controls/).
Compact-mode consumerleaderloss/cancellation rowpreparedwith unchanged20s limits;
legacy/full400k/profile/fault qualification remainpending.

### Compact metadata native journal consumer faults accepted — 2026-10-05

Executed18b8fae raceparentPASS37.58s. Actuallibraryconsumerleaderloss with4039
pending/R3 memoryAckNone admits; full1500/6000 passes1.927858s under20s.
Cancellationvisited128/contextCanceled/cleanup passes408.287928ms. Highpayload
crossesrecord/bytewindows. Libraryshutdown, notprocessSIGKILL. Source/actualSDK/
race/VCS/closure andcompleteclosedarchive independentlyverified. No400kcapacity,
legacy/fullmatrix/24h qualification ordefaultadoption.
[Faultproof](scale/compact-audit-metadata-2026-10-05/native-fault-controls/).

### Plain full400k compact metadata comparison prepared — 2026-10-05

An explicitcopied-store plaincomparison skipsprofiling overhead, recordsSDK
concurrent /compactconcurrent /SDKrecheck fullreports, elapsed/allocation/GC for
original20s attempts. Compactmustreturnexact400k/4.8M/400k fullreport within20s;
failed baselines andcandidate preserved. RestoredR5file stream/state400k/4.8M/400k
metadata readiness andoriginalcopy guards unchanged. Compile/opt-inskip passes.
Fullnative comparison pending, nodefaultadoption. Closedearlierprofile copy
reclaimed3.58GB onlyafter1013files/canonicalpushedarchive/manifest/PID/FD verification;
originalnative400k stores untouched. Additionalverifiedpushedworktreeparts sparse
recovery2.041GB recorded, canonicalGit retained.

### Compact metadata legacy2.11.17 native race comparison accepted — 2026-10-05

Executeda4a1b05 nativePASS49.91s, sharedoracle/compactmetadata full/cohort,
compaction/corruption/terminal/tombstone/snapshot/orphan cases match. Actualthree
legacyserver executablebytes checked/retained, allPIDsgone, selectedGitbeforeafter/
SDK/race/VCS independentlyreviewed. Complete1057-member/twopart archive readback.
Not400kcapacity/fullmatrix/24h qualification ordefaultadoption.
[Legacyproof](scale/compact-audit-metadata-2026-10-05/legacy-controls/).

### Plain400k compact metadata capacity failure preserved — 2026-10-05

Executedce885e0 normal2CPU/2GiB R5 freshverified copies nativeFAIL77.08s.
SDKconcurrent/compactconcurrent/SDKrecheck allhitunchanged20s deadlines;
allocated4.212/3.521/4.702GB and46/28/53GCcycles. Incompleteattemptallocations
notper-record/fullthroughput proof. Original1058storefiles unchanged, allactual
SDK/fiveNATS bytes/mounts/closedPIDs/sourcebeforeafter reviewed. Completeclosed
proof retained. No unchangednative rerun/defaultadoption/400kcapacity/24h pass.
[Plainfailure](scale/compact-audit-metadata-2026-10-05/plain-400k-comparison/).
Nextmeasure clientiterator timer/delivery overhead and explicitavailableVM CPU/GC
configuration; original fullpopulation/auditlimits remainunchanged.

### Explicit available-VM four-core capacity comparison prepared — 2026-10-05

The full plain400k two-core comparison remains failed. A fresh verified-copy
comparison is prepared with GOMAXPROCS4, GOGC200 and the same2GiB Go memory target.
Exact400k/4.8M/400k population and each original20s deadline remain required.
This jointly changes CPU/GC configuration; no isolated CPU-cause or capacity pass
is inferred. Original stores remain closed; redundant disposable-copy reclamation
is verified against pushed canonical archive parts.
[Preparation](scale/compact-audit-metadata-2026-10-05/four-core-400k-preparation/).

### Four-core full400k comparison still misses capacity — 2026-10-05

The changed configuration at1f53653 fails77.25s; SDK/concurrent, compact/concurrent
and SDK/recheck each exceed unchanged20s. Alloriginal1058files remain unchanged.
Independent source/executable/server/mount/closure review completes; fullclosed
archive preservation is running, not yet accepted. ChangedCPU/GC alone doesnot
qualify capacity. Iterator heartbeat and adapter delivery overhead are next.
[Result](scale/compact-audit-metadata-2026-10-05/four-core-400k-comparison/).

### Native delivery cost comparison and fullfour-core evidence accepted — 2026-10-05

The failed400k changedCPU/GC profile is fully archived/read back. Normal R3
delivery-only diagnostic atd0a08e6 passes6.99s, validates every100k x128Brecord
and consumercleanup. DirectNext/adapter/Consume/Nextrecheck370/380/327/361ms,
with linkedserver allocation activity included. This supports a callback delivery
candidate measurement, not a fullcapacity/integrity/fault/default adoption.
Complete1084-member archive and selectedsource/actualSDK closure reviewed.
[Delivery measurement](scale/audit-delivery-cost-2026-10-05/native-comparison/).

### Delivery diagnostic callback lifecycle race control accepted — 2026-10-05

At ff6ba0a the same full100k coordinate/payload diagnostic passes27.13s under
race, with allfour consumers removed and no race report. Fullclosed1084-member
archive/readback and selectedsource/actualSDK/race/module/closure verified.
Race timings are not performance proof. Integrated callback scanner recovery and
full400k/24h qualification remain pending.
[Race control](scale/audit-delivery-cost-2026-10-05/race-control/).

### Experimental callback scanner current native race controls accepted — 2026-10-05

At a9e29ea the explicit callback scanner passes current R3 fullaudit oracle
comparisons for compaction/cohort/corruption/state plus30MiB payload windows,
deletedsequence/cutoff and cancellation. Shared invariant/gap/replay/retry body
retained; Stop releases blockedcallback and joins within originaldeadline.
Unit race lifecycle/error/join controls pass. Complete2159-member native archive
and selectedsource/actualSDK/race/module/closure independently reviewed.
No default adoption or fullcapacity/24h pass; nativeleaderloss/legacy next.
[Controls](scale/callback-audit-delivery-2026-10-05/native-controls/).

### Callback fullaudit native consumerleader/cancellation accepted — 2026-10-05

At e792fa8 selected callback race cases pass38.70s. Library consumerleader
shutdown (not OS SIGKILL) with4039 pending leads to exact1500/6000 report in
1.938723s within original20s. Cancellation at128 returns context.Canceled in
340ms; zero auditconsumers checked. Complete closed archive/readback and selected
source/actualSDK/race/module/closure reviewed. Legacy/full400k/24h remain pending.
[Fault proof](scale/callback-audit-delivery-2026-10-05/native-fault-controls/).

### Callback legacy race compatibility and full400k comparison prepared — 2026-10-05

At010d5ef legacy2.11.17 full/cohort/compaction/corruption/state callback oracle
comparisons pass59.92s under race. Actualthree legacyserver bytes/modules and
closedPIDs plus selectedGit/actualSDK/race/closure and fullarchive readback verified.
A new explicit callbackcapacity mode retains SDK/compact baselines, requires
exact400k/4.8M/400k and eachoriginal20s deadline, and uses the samefour-core/
GOGC200/2GiB profile. Opt-in compile/skip passes; actual capacity remains pending
verified headroom. Defaults and full24h qualification remain open.
[Legacy](scale/callback-audit-delivery-2026-10-05/legacy-controls/) ·
[Capacity preparation](scale/callback-audit-delivery-2026-10-05/capacity-preparation/).

### Full400k callback candidate capacity failure — 2026-10-05

At b943074 the samefour-core/GOGC200/2GiB quietworkflow R5 profile fails97.60s:
allfour SDK/compact/callback/SDKrecheck original20s attempts expire. Callback
reports a cleanupjoin deadline and doesnot claim synchronouscleanup success.
ObservedSDK/five servers subsequently close;676 selectedGit/actualexecutables/
modules/mounts and1058 unchangedoriginalfiles independentlyverified. Fullclosed
archive preservation running, notyet accepted. Milliondiagnostic sharesVM.
No default/fullcapacity/24h promotion; integrated per-record reduction/delivery
cost is next. [Failure](scale/callback-audit-delivery-2026-10-05/capacity-400k/).

### Callbackcapacity archive complete; fullcardinality CPU reduction profile prepared — 2026-10-05

Fullclosed callback400k failure archive/readback is complete. A CPU-only full
400k/4.8M template profile separates decode, predecoded protocol/map reduction
and combined decode/reduction with production sorted finish. Exactcounts required;
reused encodedbytes/contiguoussubjects/constantterminal lookup and profiling are
explicit. No transport/INV/KV/fault/retained-state or capacity claim.
[CPU preparation](scale/audit-reduction-cost-2026-10-05/preparation/).

### Fullcardinality isolated reduction CPU evidence accepted — 2026-10-05

At6adad6e instrumented CPU-only diagnostic passes7.69s: full4.8M JSON decode
2.856s, predecoded protocol/maps/sortedfinish1.047s, combined3.737s. Bothreduction
phases report400k journals/4.8M entries/400k terminals; no invocation scan.
Source/executable/module/closure and full696-member archive/readback verified.
Reused rawbytes/contiguoussubjects/constantterminal lookup/noINV/KV/transport/
faults explicit. This supports measuring bounded delivery buffering/pipeline
cost next, without claiming isolatedcause or nativecapacity/24h/default adoption.
[Profile](scale/audit-reduction-cost-2026-10-05/profile/).

### Bounded callback queue and failed native measurement preserved — 2026-10-05

Explicit candidate adds256records/1MiB payload queue or one oversized record to
the retained SDK8MiB pullbuffer; unit race budget/credit/stop controls pass. Native
83d318b six-reader100k diagnostic fails7.91s on consumerCount1 after API deletes,
with allsix deliveries logged. No convincing speedup or confirmed residualcause.
Full1088-member failure archive and selectedsource/SDK/closure verified, including
initial wrong PASS-only reviewer. A changed diagnostic records names/counts per
delete within original30s reader context. No buffered/fullcapacity/default/24h
promotion or unchanged rerun.
[Failure proof](scale/buffered-callback-audit-2026-10-05/failed-delivery-measurement/).

### Buffered queue rejected after observed-cleanup measurement — 2026-10-05

At e43d1b3 changed diagnostic passes7.89s, with allsix100k deliveries and
per-delete count0/namesempty within original30s contexts. Previouscount1 failure
is preserved; no originalcause confirmation. Buffered390ms vsunbuffered257ms
doesnot support adopting a queue. Experimental queue moves to test-only;
production callback source restored exactly to a39bb6c, unit race lifecycle/
byte/compact controls pass1.216s. Full1087-member archive/readback and selected
Git/actualSDK/module/closure reviewed. Defaults/full400k/24h still open.
[Cleanup measurement](scale/buffered-callback-audit-2026-10-05/observed-cleanup-measurement/).

## Quiet cursor replication diagnostic preparation — 2026-10-05

Explicit temporary R5/R1/R5 cursor comparison is prepared with actual consumer
configuration observations, full400k/4.8M cardinalities and original20s deadlines.
No native result/default adoption/fault qualification yet. Lossless base-plus-delta
fixture preservation passes four reconstruction tests and full committed400k base
readback (1751 data files / 3670153990 bytes).
[Preparation](scale/single-replica-audit-cursor-2026-10-05/preparation/).

## Full400k R5/R1 cursor capacity failure preserved — 2026-10-05

The changed quiet R5/R1/R5 callback comparison at f2361d9 fails77.24s; all modes
miss unchanged20s full400k/4.8M gates. Actual cursor configs5/5,1/1,5/5 verified.
R1 reaches final validation and reduces219008 journals/2628096 entries before
deadline; no default/fault/legacy/24h adoption. Selected source/actual SDK/five
servers/mounts/closure and1058 unchanged original store files reviewed. Complete
1698-file logical fixture preserved with verified49.95MB delta plus pinned
canonical base (824 unchanged file references). Next changed measurement targets
R1 scan/state/final-validation phase costs; no unchanged rerun.
[Result and preservation](scale/single-replica-audit-cursor-2026-10-05/capacity-400k/).

## R1 phase diagnostic preparation and canonical copy reclamation — 2026-10-05

Full pushed base-plus-delta/current copy hashes and all visible task descriptors
verified before reclaiming993 closed disposable store files /3580353716 bytes;
original stores and complete pinned proof retained. A changed R1 full400k profile
records scanner/visitor timing, CPU/alloc samples and direct state-watch lifecycle
without relay overhead. Initial-set contract race controls pass1.023s; native
measurement pending. Original20s gate/full cardinalities remain unchanged.
[Preparation](scale/single-replica-audit-cursor-2026-10-05/phase-preparation/).

## Instrumented R1 journal delivery cost localized — 2026-10-05

Full400k R1 profile at6184597 diagnosticPASS36.43s retains auditFAIL20.004s plus
cleanup deadline: INV1.158s, JRN18.844s/4.190M of4.8M records, visitors8.040s;
state-watch stop finishes3.001s from audit start. No state barrier/capacity claim
from lifecycle alone. Profiling adds cost; earlier unprofiled final reduction
progress is not reproduced under instrumentation. SDK samples show parser/select/
message allocation and visitor work, without server-defect attribution.
SelectedGit/actual SDK/five servers/closure/original1058 files and complete1708-file
base-plus-delta inventory independently verified. Next changed candidate removes
one per-record adapter handoff while retaining shared invariant/recovery logic.
[Full profile](scale/single-replica-audit-cursor-2026-10-05/phase-profile/).

## Direct callback windows prepared — 2026-10-05

Explicit direct callback candidate removes the extra batch relay goroutine/channel
while retaining shared bounds/gap/order/replay/two-resume/invariant logic and
4096-record/2s windows. Original adapters still drain their channels. Race unit
and differential/cancellation/error/cursor controls pass1.034s; R3 native full
correctness comparisons prepared, no default/capacity/fault/legacy/24h acceptance.
Previous closed profile copy reclaimed after complete canonical verification.
[Preparation](scale/direct-callback-audit-2026-10-05/preparation/).

## Direct callback native full correctness controls accepted — 2026-10-05

At8b9b48b allfour R3 native race suites pass: compaction/cohort/fresh corruption,
journal corruption, state values and116x256KiB payload refill/holes/cutoff/cancel.
Direct callback matches independent point oracle reports/errors/digests; cancellation
visits1 and consumers0. Selected680 Git inputs/actual SDK/race/module/closure and
complete2163-member archive readback independently reviewed. Race times not speed
proof; no default/R1 recovery/full400k/legacy/24h claim. Native consumer-leader-loss
and cancellation fault controls prepared next.
[Native proof](scale/direct-callback-audit-2026-10-05/native-controls/).

## Direct callback consumer-fault controls accepted — 2026-10-05

At4f47f0b nativeR3 race controls pass37.05s: consumerleader library shutdown with
4039pending, complete1500/6000 audit in1.946858s; cancellation128/cleanup354ms,
zero audit consumers asserted. Selected source/actual SDK/race/module/closure and
complete1427-member archive independently reviewed. No OS SIGKILL/R1 failover/
legacy/full400k/default/24h claim. Legacy and capacity qualification remain next.
[Fault proof](scale/direct-callback-audit-2026-10-05/native-fault-controls/).

## Direct callback legacy accepted; fullcapacity prepared — 2026-10-05

At1bf66ca legacy2.11.17 native race comparisons pass67.84s. Full/cohort/compaction/
corruption/state point oracles include direct callback; actual SDK/three server
executable hashes and closedPIDs independently verified, complete1065-member
archive read back. FixedR1 callback/direct/callback full400k comparison prepared;
original20s/4.8M cardinality unchanged. Compile/skip preparation passes. No R1
failover/default/fullcapacity/24h claim.314345594 bytes of redundant temporary
proofs reclaimed only after pushed canonical part/base verification and descriptor
checks; stores/source/media untouched. Fullcapacity launch awaits disk headroom.
[Legacy proof](scale/direct-callback-audit-2026-10-05/legacy-controls/).
[Capacity preparation](scale/direct-callback-audit-2026-10-05/capacity-preparation/).

## Full400k direct callback capacity failure preserved — 2026-10-05

FixedR1 callback/direct/callback comparison at4ea20c1 fails77.37s; all original20s
full400k/4.8M gates missed. First callback reaches6469 journal checks/77616 entries/
6468 terminal before terminal lookup context deadline; no stored-state absence
claim. Direct/recheck expire before final reduction. Actual R1 configs/starts
verified. Selected source/actual SDK/five servers/mounts/closure and1058 unchanged
original store files independently reviewed; complete1685-file logical fixture
preserved in verified49.998MB delta plus pinned canonical base. No speedup/default/
R1 failover/fullcapacity/24h claim. Next changed measurement considers enlarged-VM
CPU/GC headroom while retaining fullpopulation/deadline and recording configuration.
[Full failure proof](scale/direct-callback-audit-2026-10-05/capacity-400k/).

## Enlarged-VM audit GC headroom prepared — 2026-10-05

Explicit4-core/GOGC500/4GiB profile prepared against exact400k/4.8M population and
original20s deadlines, adding runtime GC/heap/pause/actual memory-limit metrics
and sampled SDK RSS/highwater. Compile/skip passes; native result pending. Earlier
2GiB/GOGC200 failures remain failed; no R1 recovery/default/24h claim. Last closed
copy reclaimed only after full canonical and byte/descriptor/process verification.
[Preparation](scale/direct-callback-audit-2026-10-05/gc-headroom-preparation/).

## Full400k explicit GC-headroom capacity accepted — 2026-10-05

At08a90c8 mandatory directR1 quiet audit passes18.350496s with exact400k INV,
journals and terminal /4.8M entries under4-core/GOGC500/4GiB. Actual4GiB runtime
metric and R1 cursor configs verified. NativePASS74.00s includes two failed20s
callback controls; no blanket pass/speed ratio or earlier2GiB promotion. Selected
source/actual SDK/five servers/mounts/closure/1058 unchanged original files and
complete1686-file base-plus-delta inventory independently verified. R1 cursor/
server loss/replay, cancellation/live/default adoption and actual24h remain open.
[Full evidence](scale/direct-callback-audit-2026-10-05/gc-headroom-capacity/).

## Direct R1 native loss controls prepared — 2026-10-05

A new native1500/6000 directR1 mode prepares three independent owner-library-
shutdown, acknowledged consumer-deletion and cancellation fixtures. Actual cursor
identity/replicas/pending tail verified; fullreport/visits/original20s and consumer
cleanup gates retained. Existing replay/transport/retry rules unchanged. Race
compile/unit controls pass1.037s; native verdict pending and either outcome will
be preserved. Previous closed fullcapacity copy reclaimed after complete canonical
verification. No R1 failure/default/R5/24h qualification yet.
[Preparation](scale/direct-callback-audit-2026-10-05/single-replica-fault-preparation/).

## R1 shutdown failure localized and typed-status correction prepared — 2026-10-05

Initial R1 fault parent8000503 FAIL53.17s: confirmed cursor-owner library shutdown
with4423 pending,1947 accepted records then SDK ErrServerShutdown. The transport
classifier omitted this typed status. Cancellation128/135ms and consumer-deletion
complete1500/6000 in1.369s pass independently; parent remains failed. Full1791-member
archive independently reviewed/read back. Exact typed shutdown now enters existing
bounded transport recovery; visitor/untyped/semantic errors stay fatal. Race
controls pass1.040s; changed three-fixture native qualification prepared. No R1
failure/default/R5/largepopulation/24h claim.
[Original failure](scale/direct-callback-audit-2026-10-05/single-replica-native-fault-controls/).
[Correction](scale/direct-callback-audit-2026-10-05/single-replica-recovery-preparation/).

## Focused R1 shutdown/deletion/cancellation recovery accepted — 2026-10-05

Changed reader atd779882 native racePASS55.04s: actualR1 owner library Shutdown
with4374 pending recovers exact1500/6000 report in1.505354s; acknowledged deletion
with4121pending recovers in1.309517s; cancellation128/97ms, all zero consumer
assertions. Exact SDK shutdown sentinel now enters existing bounded recovery;
visitor/untyped/semantic errors remain fatal and replay rules unchanged. Original
8000503 failure stays preserved. Independent source/actual SDK/module/closure and
full1791-member archive readback verified. R5 process SIGKILL/same-owner restart/
larger faultcapacity/live/default/finalmatrix/actual24h remain open.
[Evidence](scale/direct-callback-audit-2026-10-05/single-replica-shutdown-recovery/).

## Actual R5 process-owner loss qualification prepared — 2026-10-05

The explicit R1 direct reader now has fresh five-container SIGKILL controls with
R5 file INV/JRN/STATE sources. The actual pending R1 cursor owner is verified at
visit128, then killed with observed source exit. Separate fixtures leave it down
or restart its same node/store. Exact1500/6000 baseline and recovery, contiguous
once-only journal visits, original20s including fault/restart, and zero audit
consumers are required. No replay/retry/default change. Native verdict and archive
review pending; full fault capacity/live/legacy/final matrices/24h stay open.
[Preparation](scale/direct-callback-audit-2026-10-05/r5-process-preparation/).

## R5 process restart replay failure preserved — 2026-10-05

Actual five-container controls at69f2dc7 FAIL334.46s: owner-left-down audit completes
1500/6000 in4.040516s but post-audit stream-info request expires at outer5m context;
independent surviving-server requests report zero audit consumers. Same-store
restart rejects sequence1 replay after1961accepted journal entries,3.563312s.
681 selected inputs/actualSDK/11actualserver processes/mounts/closure and full
1777-member archive independently verified. Neither parent qualifies. Changed
fresh diagnostic records actual consumer API positions/identity at replay proof
and uses short cleanup requests within original20s. Strict replay unchanged.
[Failure](scale/direct-callback-audit-2026-10-05/r5-process-owner-loss/).
[Diagnostic](scale/direct-callback-audit-2026-10-05/r5-replay-diagnostic-preparation/).

## Independent retained cursor cleanup prepared — 2026-10-05

eb0f5ca diagnosticFAIL70.11s: left-down audit complete1500/6000 then WF_JRN
consumer count1 through original20s; same-store restart passes in that schedule
via new cursor1962, without reproducing/explaining prior overlap. Full1667-member
archive/source/actual processes/closure independently reviewed. Shared scanner
sequential deletes use one2s context: unanswered old-owner deletion can starve
replacement cleanup. Independent deletions now join under unchanged2s/original
deadline; targeted race regression/controls PASS1.209s after synchronizing fake
bookkeeping. Fresh native per-name deletion diagnostic prepared. Replay unchanged.
[Diagnostic](scale/direct-callback-audit-2026-10-05/r5-replay-diagnostic/).
[Cleanup preparation](scale/direct-callback-audit-2026-10-05/r5-cleanup-preparation/).

## Verified R1 volatile position loss recovery prepared — 2026-10-05

7d79d13 owner-left-down native fixture passes exact1500/6000 with zero consumers
at4.058s; old delete expires while independent replacement delete succeeds.
ParentFAIL52.73s on same-owner restart overlap. Actual cursor name/created/config/
owner unchanged, API delivery and AckNone floor regress1961→1126 behind accepted
source position. Full1666-member/source/actual process/closure proof reviewed.
Explicit directR1 reader now admits only fresh API-confirmed positive delivery/
floor regression with identical complete config/identity/owner, no outstanding
acks/redeliveries. Common recovery creates fresh cursor at unvisited sequence;
ordinary overlap stays fatal. Race controls PASS1.207s; fresh R5 verdict pending.
[Cleanup/failure evidence](scale/direct-callback-audit-2026-10-05/r5-independent-cleanup/).
[Recovery preparation](scale/direct-callback-audit-2026-10-05/r5-position-recovery-preparation/).

## Actual R5 SIGKILL/same-store R1 position recovery accepted — 2026-10-05

Executed889a70c native racePASS61.26s, both fresh1500/6000 fixtures. Owner-left-down
audit4.341273s/zero consumers4.344642s; same-store restart audit2.409395s/zero
consumers2.412256s. Restart actually observes same assignment/config/owner API
position1961→1726, then fresh cursor1962. Exact contiguous once-only journal visits,
full reports/original20s/unchanged2s cleanup/two-resume/gap proof hold. Independent
683selected inputs/actualSDK/11server processes/module/mounts/closure and full
1668-member69.327MB archive/readback reviewed. Prior failures remain failed;
large fault capacity/legacyR1/live/default/fullmatrix/actual24h stay open.
[Evidence](scale/direct-callback-audit-2026-10-05/r5-position-loss-recovery/).

## Actual R1 legacy cursor oracle compatibility prepared — 2026-10-05

Common full-oracle native comparisons now include explicit directR1 with actual
server-verified memory/AckNone replica1 config against independent point reports/
errors. Fresh three-process2.11.17 legacy test will cover full/cohort/compaction/
corruption/state/tombstone/orphan cases with R3 sources and fallback provisioning.
Original20s/public defaults unchanged. Compile/skip passes; native verdict and
complete closed fixture proof pending. Legacy process faults/largefault/live/
default/final matrices/actual24h remain open.
[Preparation](scale/direct-callback-audit-2026-10-05/r1-legacy-preparation/).

## Actual R1 legacy compatibility accepted; full fault capacity prepared — 2026-10-05

26c33ad actualthree-process2.11.17 native racePASS72.45s, all18 actualR1 cursor
observations againstR3 sources, exact full/cohort/compaction/corruption/state/
snapshot/orphan point-oracle reports/errors. Independent683selected inputs/
actualSDK/three server executable/module/PID closure and full1067-member48.895MB
archive/readback accepted. Legacy processfault/live/default/fullmatrix/24h open.
Full400k/4.8M copied capacity harness now admits explicit owner-down/restart, each
on fresh fullcopies, current-source healthy baseline before fault, allR5 replicas
caught up, actual pending cursor owner SIGKILL/restart proof, contiguous once-only
entries/exactreport/zero consumers under original20s each. API/deletion/cleanup/
allocation/GC/actualmemoryCPU observations persist before verdict. Compile/skip
passes; native pending sufficient fresh-copy+archive disk. Smaller populations
will not certify this full fault gate.
[Legacy](scale/direct-callback-audit-2026-10-05/r1-legacy-controls/).
[Full capacity faults](scale/direct-callback-audit-2026-10-05/full400k-fault-preparation/).

## Full400k owner-loss execution headroom restored — 2026-10-05

Supported older canonical archive schemas expose84 closed duplicate combined
proofs. Fresh primary GitHub terminal job/run states, exact pushed metadata/part/
concat hashes and visible task descriptors verified before reclaiming4,756,862,989
bytes. Original stores/provider media/source/executables/livecampaign/caches/Git
parts retained. Full400k/4.8M owner-left-down producer prepared with current-source
baseline, actual R5 owner SIGKILL, original20s per baseline/fault including cleanup,
4CPU/GOGC500/4GiB and whole donor/source/actual process verification. No verdict yet;
each outcome will receive complete pinned base-plus-delta review/preservation.
[Producer](scale/direct-callback-audit-2026-10-05/full400k-owner-down-preparation/).
[Reclamation proof](scale/direct-callback-audit-2026-10-05/full-capacity-proof-headroom/).

## Current-source full fault baseline failed; cold CPU/GC diagnostic prepared — 2026-10-06

bb6f75c normal full400k/4.8M nativeFAIL36.88s: healthy baseline accepts4256199
journal visits then original20s/callback-join deadlines; no fault injected.
Actual4CPU/4GiB/R1 and R5 file/catch-up verified. Partial2.815GB/7GC is incomplete
attempt data, not isolated causality/per-record speed. Full1710-file/823alias
pinned base+50.100MB delta and selectedSDK/server/mount/closure/1058 unchanged
donor files independently reviewed. Earlier ordered/warmed18.35s remains scoped.
Changed fresh-copy optionalCPU profile adds stream boundaries/heap/NextGC/pause/
watch lifecycle with no per-record clock/update relay, hoisted constant lookup and
500ms observer (no replacement PID in owner-down). Original20s/fullscope/config
unchanged; compile/skip passes, native pending verified fresh-copy headroom.
[Failure](scale/direct-callback-audit-2026-10-05/full400k-owner-down/).
[Preparation](scale/direct-callback-audit-2026-10-05/full400k-cold-profile-preparation/).

## Cold full400k CPU diagnostic verified — 2026-10-06

Fresh clean4086e44 normal4CPU/GOGC500/4GiB full400k/4.8M baseline fails original20s
after4,282,850 journal visits; owner fault never injected. Complete1711-file/819alias
lossless proof and unchanged1058 donor files independently verified. CPU profile
shows overlapping cumulative JSON decode5.02s/client parse4.09s/select2.69s;
GC pause21.295ms does not establish isolated causality. Successful Watch Stop alone
is not initial-set barrier proof. Measure bounded handoff/decoding alternatives;
retain all semantic/recovery/cleanup/cardinality/deadline gates. No default,
full fault, current matrix, or24h qualification. [Evidence](scale/direct-callback-audit-2026-10-05/full400k-cold-profile/).

## Explicit bounded chunk handoff prepared — 2026-10-06

Measured full-capacity client select/decode costs motivate amortized handoff.
Private chunk candidate keeps visitors on scanner goroutine and shares all gap,
semantic, recovery and cleanup gates. Two bounded256-record/1MiB payload buffers
(or one oversized record each), SDK/in-flight/header/object accounting separate.
Race pressure/order/cancel/error/join controls accepted; native legacy oracle and
fresh full400k/4.8M original20s CPU/fault qualification pending. No adoption or
speedup claim. [Preparation](scale/chunked-callback-audit-2026-10-06/preparation/).

## Chunked R1 legacy oracle accepted — 2026-10-06

Clean578c216 actualthree-process NATS2.11.17 race full-oracle testPASS74.53s.
All18 actualchunked R1 cursors and full/cohort/compaction/corruption/state/tombstone/
snapshot/orphan reports/errors match independent point oracle. SDK/source/actual
legacy bytes/closedPIDs and whole1070-member48,939,101byte archive/readback accepted.
Full400k/4.8M original20s cold CPU/owner-down gate remains pending; no default/live/
fullmatrix/24h qualification. [Evidence](scale/chunked-callback-audit-2026-10-06/legacy-controls/).

## Full chunked audit completes; residual cleanup gate fails — 2026-10-06

Atacc9123 fresh full400k/4.8M normal4CPU/GOGC500/4GiB exact read/reduction completes
17.114920s. Both created deletes succeed, INVcount0 observed, JRNcount2 persists to
original20s overall deadline. NativeFAIL37.33s; no owner fault injected. Residual
identity/cause unconfirmed; capture pre-audit named configs and post-delete named
APIs before changing fixture/cleanup semantics. Complete1713-file/827alias lossless
proof, actualsource/SDK/fiveservers/closure/unchanged1058donorfiles verified.
No combinedcleanup/fault/default/live/fullmatrix/24h acceptance.
[Evidence](scale/chunked-callback-audit-2026-10-06/full400k-cold-profile/).

## Named full-capacity cleanup diagnostic prepared — 2026-10-06

Capture pre-audit full consumer identities/configs and first residual count names,
plus each current cursor's named API/typednot-found. Preserve zero assertion,
original20s/full400k/4.8M; do not delete restored consumers or infer cause from
unnamed count. Compile/opt-in skip accepted; new fresh-copy execution pending.
[Preparation](scale/chunked-callback-audit-2026-10-06/named-cleanup-preparation/).

## Copied pre-audit consumer count confirmed — 2026-10-06

Fresh058040c full400k/4.8M preflightFAIL16.30s before any current cursor or bulk
scan. INVcount0/list empty; JRNcount2 pre-existing, Info listtimes out1s. Doesnot
prove identities/liveassignments/servercause. Full1714file830alias proof and
source/SDK/fiveservers/closure/unchanged1058donorfiles verified. Initial review
failure retained/corrected. Next bounded probe captures ConsumerNames before Info;
resolve fixtureprecondition before more bulk runs. No deletion/zeroassertion
relaxation/capacity/fault/default/24h acceptance.
[Evidence](scale/chunked-callback-audit-2026-10-06/initial-consumer-failure/).

## Restored residual cursor identities confirmed — 2026-10-06

Fresh53b3871 exact400k/4.8M read/reduction completes17.473097s. Before current
audit, two old R5 memoryAckNone cursors exist (createdOct5 19:54/19:55). Post-delete
names match them; current R1 JRN cursor is typed404/10014 missing, both current
deletes succeed. Overallzero gate remainsFAIL20s. Complete1714file828alias proof
verified. Prepare freshcopy by explicitly validating/deleting only confirmed old
names, recording boundaries/populations and requiring zero before original20s
healthy/fault gates. Originaldonor untouched; no fault/default/live/24h pass.
[Evidence](scale/chunked-callback-audit-2026-10-06/assignment-names/).

## Verified restored-cursor fixture preparation — 2026-10-06

Only exact two independently captured old names/timestamps/config/position0 are
eligible for freshcopy preparation deletion; unknown/extra/changed/unavailable
identity aborts before deletion. Record initial/delete/finalnamedzero and identical
retained message/byte/first/last/deletion boundaries. Existing60s readiness includes
5s inventory probes; no journalwarmup/20s/full400k/4.8M changes. Race validation/
pressure controlsPASS1.024s. Native baseline/actualowner-down pending headroom;
original donor/failed fixtures remain unchanged. No default/live/fault/24h pass.
[Preparation](scale/chunked-callback-audit-2026-10-06/clean-fixture-preparation/).

## Full400k cold and owner-left-down capacity accepted — 2026-10-06

At02d485a cleanfreshcopy nativePASS51.57s: exact400k/4.8M baseline audit18.132709s/
zero18.134257s, actualR5source/R1ownerSIGKILL leftdown audit16.427917s/zero16.429780s.
Distinct resume24202 after24201 and once-only4.8M visits; original20s retained.
Preparation only deletes validated old donor cursors and proves zero/unchanged
source boundaries. Whole1698file823alias proof/actualSDK688inputs/fiveservers/
mounts/closure/1058unchangeddonorfiles accepted. Fullsame-store restart/default/
live/currentmatrices/actual24h remain open. [Evidence](scale/chunked-callback-audit-2026-10-06/clean-fixture/).

## Full400k same-store cursor-owner restart accepted — 2026-10-06

Fresh8693ab5 nativePASS53.85s: verified/prepared400k/4.8M baselinezero17.394841s,
actualR5source/R1owner node1SIGKILL/samestore restart zero17.087980s. Distinct cursor
129 after128; exact reports/all4.8M once-only visits withinoriginal20s. Six actual
closedserverprocesses/old-newownerPIDs+containers/samemount/SDK688sourceinputs/
unchanged1058donorfiles/full1699file816alias proof accepted. This run resumes on
transport failure, not a same-assignment position regression proof. Public/default/
live/currentmatrix/actual24h remain open. [Evidence](scale/chunked-callback-audit-2026-10-06/full400k-owner-restart/).

## Explicit chunked-concurrent live entry points prepared — 2026-10-06

Full/cohort explicit public APIs and matrix checkpoint/final mode select same
bounded chunk/R1 scanner/state snapshot/common checks. Actual cursor config/name
validated; source replication unchanged. Produceropt-in requires4GiB and selects
4CPU/GOGC500, matching recordedfullcapacity profile. Four readers mutuallyexclusive;
20s/60s/threeattempts/p99heal/drain/cardinality unchanged. TenPython controls plus
Go race retry/conflict/callback/position controlsPASS1.022/1.029s. Current2.15library
race fulloracles prepared; sustainedlive/default/currentmatrix/24h remainsopen.
[Preparation](scale/chunked-callback-audit-2026-10-06/live-entry-preparation/).

## Current chunked entry oracle equivalence accepted — 2026-10-06

At9eaf35f actual2.15library race compaction/cohort/freshcorruption75.41s,
journalcorruption13.81s/statevalues13.40s PASS. All41 actualR1 entryscanner
observations sourceR3/cursorR1, complete reports/errors match point oracle.
ActualSDK/module/688inputs/cleanVCS/closure/full1772member22,993,426byte archive
readback accepted. Live runner recordsCPU/GC profile. Next explicitnormal10m
R5journal/repeatedSIGKILL/checkpoint/final/drain gate matchesqualified4CPU/GOGC500/
4GiB; original20s/60s/threeattempts unchanged. No live/default/matrix/24h pass.
[Evidence](scale/chunked-callback-audit-2026-10-06/current-oracles/).

## Explicit chunked ten-minute journal launch verified — 2026-10-06

Live clean2f74289 normalSDK799942 actual executable/VCS and /proc4CPU/GOGC500/
4GiB/chunked mode verified, five R5 node roles observed. Seed1 ten-minute journal
campaign/repeatedSIGKILL/checkpoint/final/drain gates retain20s/60s/threeattempts.
Supervised watcher/independent terminal review active; no live stores read/copied.
No sustained/default/currentmatrix/24h pass from launch metadata.
[Launch](scale/chunked-callback-audit-2026-10-06/live-journal-initial/).

## Actual24h chunked campaign preparation — 2026-10-06

Normal4CPU/GOGC500/4GiB R5 journal seed1 with original20s/60s/threeattempts,
checkpoint/final/drain gates prepared after ten-minute qualification. Actual
24h observer/reviewer deadlines retain source/executable/original archive and
independent row verification. Verified pushed duplicate archive/worktree parts
reclaimed with canonicalGit/originalfixtures/cache retention, about9.4GiB free.
Preparation alone does not qualify actual24h/default/fullmatrix.
[Preparation](scale/chunked-callback-audit-2026-10-06/live-journal-24h-preparation/).

## Explicit chunked live journal ten-minute row accepted — 2026-10-06

Clean2f74289 normalPASS653.11s,94 batches/2632invocations/29003entries,19actual
journal leaderSIGKILL/heals,9checkpoints/finalwholeintegrity/history/physicaldrain.
Original20s/60s/threeattempts and30sp99 retained; normal4CPU/GOGC500/4GiB.
ActualSDK/server/source/originalarchive closure and complete100,804,049byte proof
independently verified/readback. This explicit row is qualified; actual24h,
defaultreader and currentfullmatrix remain open.
[Evidence](scale/chunked-callback-audit-2026-10-06/live-journal-ten-minute/).

## Actual24h chunked journal launch verified — 2026-10-06

Clean21b4729 actualSDK862338 normal24h journalseed1 executing under original gates;
1628sourceinputs/actualexecutable/VCS/profile4CPU/GOGC500/4GiB verified, allfive
currentR5 server roles observed. Named user systemd producer/observer/reviewer live.
First actual leaderSIGKILL/heal recorded, no live stores read/copied. Terminal
24h/default/fullmatrix qualification remains open.
[Launch](scale/chunked-callback-audit-2026-10-06/live-journal-24h-initial/).

## Bounded final point latency audit prepared and native oracle accepted — 2026-10-06

Non-clock R5 final phase schedules32independent original20s point audits under
sameparentdeadline; allinvocation/journal/enablingevent/rollout checks and ordered
output retained, errors cancel/join and invalidate wholeoutput. Racecontrols pass.
Cleana84c879 currentR3libraryrace160actualshort/timer/signal/parent/child invocations
match serialpointoracle exactly; impossibleterminaldeadline failsboth, native16.81s.
ActualSDK/source/closure/complete31,740,808byte proof independently verified/readback.
Changedsource normalten-minute adoption prepared; original24h pinnedsource unchanged.
No fullscale/fault/currentmatrix/24h qualification from nativeoracle alone.
[Evidence](scale/parallel-final-latency-2026-10-06/native-oracle-reviewed/).

## Changed final audit ten-minute launch verified — 2026-10-06

Clean89642e4 actualSDK920976 normalR5 journal seed1 ten-minute campaign running
with bounded32finalpoint latency reads, explicitchunked4CPU/GOGC500/4GiB and
originalgates. Actualexe/VCS/profile/fivecurrentserver roles verified, user
systemd observer/reviewer live. Original24h source21b4729 remainsrunning. Terminal
sustained/fullscale/fullmatrix/24h acceptance remainsopen; no live stores read/copied.
[Launch](scale/parallel-final-latency-2026-10-06/live-journal-initial/).

## Bounded final latency sustained journal row accepted — 2026-10-06

Clean89642e4 normalPASS633.19s,90batches/2520terminals/27752entries/19leaderSIGKILLs,
9checkpoints/finalintegrity/history/physicaldrain. Bounded32finalpoint reads and
explicit4CPU/GOGC500/4GiB chunked reader retainallchecks/ordered samples/original
20s/60s/threeattempts/30sp99. Actualsource/SDK/servers/originalarchive/closure and
complete102,872,551byte proof independently verified/readback. Fullscale/default/
currentfullmatrix/24h remainopen. Larger87920cohort initialreadiness mismatch
failsbeforepoint reads; complete364,052,563byte failureproof preserved. R5fixture
readiness corrected, same60s. Cleancheckout launch rejection separate; noSDK or
copy created. Fresh verified copies required for changedmeasurement.
[Accepted row](scale/parallel-final-latency-2026-10-06/live-journal-ten-minute/).
[Failed admission](scale/parallel-final-latency-2026-10-06/retained-cohort/).

## Corrected real87920 cohort point measurement live — 2026-10-06

Clean38a0a8e actualSDK1054824/exe/VCS/profile4CPU/GOGC500/4GiB/fivealive actual
servers verified; correctedR5ready passes. Original6min point allowance/20s requests/
lastheal+5min terminaldeadline retained. Freshverifiedcopies only, oldfailedcopy
reclaimed aftercanonicalpushedproof/fullhashes/closure/FDchecks. Original24h and
milliontimer remainlive. Partialprogress is notfullsize/currentmatrix/24h proof.
[Launch](scale/parallel-final-latency-2026-10-06/cohort-r5-live-initial/).

## Larger point audit deadline failure; metadata reuse prepared — 2026-10-06

CorrectR5 real87920 copiedcohort 38a0a8e nativeFAIL377.62s;83,924successful point
callbacks beforeoriginal6min expires. Incomplete result discarded, retainedcheck
skipped; zero report notdataabsence. Complete353,988,356byte source/executable/
servers/originalstore/closure proof independently verified/readback. Newexplicit
cohort option shares successfulmetadatahandles only, allrecord/snapshot/timestamp
operations fresh; retry/cancel semantics and6min/20s checks retained. Racecontrols
pass1.061s; nativecached/uncached/serialoracle prepared. No fullscale/default/
currentmatrix/24h qualification from preparation; existing24h source unchanged.
[Failure](scale/parallel-final-latency-2026-10-06/retained-cohort-r5-v2/).
[Preparation](scale/cached-final-latency-2026-10-06/preparation/).

## Metadata reuse native oracle accepted; fresh larger measurement prepared — 2026-10-06

Clean037bed8 actualcurrentR3libraryrace160point samples matchoriginalserial and
uncachedparallel exactly, impossibledeadline stillfatal; native18.46s. Top-level
JRN/SIG/STATEhandlelookups1each, no cachedvalues/networkcount/speedratio claim.
SDK/source/closure/complete32,217,100byte proof verified/readback. Fresh real87920
cachedmetadata measurementprepared withR5ready/original6min/20s/terminaldeadline;
no original/failedstore reopen. No fullscale/default/currentmatrix/24h qualification.
[Oracle](scale/cached-final-latency-2026-10-06/native-oracle/).
[Preparation](scale/cached-final-latency-2026-10-06/cohort-preparation/).

## Explicit final point metadata reuse in live qualification — 2026-10-06

`run-tier3-soak.py --cached-latency-metadata` now selects successful metadata
handle reuse for non-clock final point and rollout audits. Each record, snapshot
and timestamp query remains fresh; original ordered reduction, cancellation,
20-second request limits and parent deadline remain. The final audit writes
`latency-metadata.json` with diagnostic lookup attempts. Clock controller rows
reject the option because they use a different audit path. The producer records
the explicit option and strips inherited activation.

All 11 soak producer controls pass; the selected Go metadata/parallel controls
pass in 0.047 seconds and compile the integration harness. Native cached/serial
oracle acceptance is already retained. Large real-cohort and sustained changed
live-source qualification remain pending; existing isolated campaigns continue.

## Real cached point audit cohort accepted — 2026-10-06

At clean14cea25 all87,920 real-workflow point checks and terminals complete in
319.641087962s underoriginal6min/20s contexts; complete969,925-entry retained report
passes original20s. Actualsource/SDK/profile/five servers/mounts/closure/unchanged
originals and full4,787member363,467,422byte proof/14parts readback verified.
Data values remain fresh; lookups diagnostic1JRN/1SIG/1STATE/0OBJ. Sustained changed
live-source/full400k final point/currentmatrix/24h gates remain open.

## Cached final point sustained journal qualification — 2026-10-06

At727b950 explicit cached metadata/chunked reader normal4CPU/GOGC500/4GiB R5
journal seed1 ten-minute PASS633.03s:2,240terminals/24,707entries/19actual leader
SIGKILL/heals/eight checkpoints/final integrity/history/physicaldrain. Worst
terminal12.329512s/progress7.208195s under30s; original20s/60s/threeattempts andfresh
data queries retained. ActualSDK/source/profile/originalarchive/servers/closure
and independent row result/full1,678member99,886,746byte proof/fourparts verified.
This explicit changedsource row qualifies; full400k final point/currentmatrix/
defaultretainedreader/original24h gates remain open. Existing long handles unchanged.

## Original retained Tier2 full ten-minute pause seed accepted — 2026-10-06

At d893bb0 normal native637.04s:2,324terminals/25,675entries/ten active45-second
pauses, exact post-resume paused invocation+epoch fencing and all workload cells/
checkpoint/final integrity/history/p99/physicaldrain. Actual SDK/three workers/
three NATS binaries/closure/694Go-module/3,286external inputs/full6,323member native
proof verified. Initial10m/10m0s reviewer mismatch preserved; corrected exact600s
review passed without rerun. Independent freshcopy full report/histories/allthree
physicalqueues/64durables pass,451.842ms audit/4.459724s combined under20s; all2,281
original files unchanged,1,698inputs/actualSDK/three NATS/closure/full4,006member
49,939,893byte copiedproof verified. Retained original sustained pause seed1 now
qualifies at executedsource. Full13x200/currentfullmatrix/original24h remain open.

## Original retained Tier2 reply isolation seed1 qualified — 2026-10-06

Normal ten-minute executed85c4cf2: PASS649.01s,82batches/2,296terminals/25,336entries,
ten active45-second reply holds, exact selected-delivery fencing and fresh PING
recovery, all six workload cells/native integrity/history/drain. Reusable review
checks actual SDK/threeworkers/threeservers closure and694Go/module/3,286external
inputs. Archived initial external-count reporting error preserved and independently
corrected. Separate fresh-copy helper73bbd85 verifies exactfull retainedreport,
histories/allthreephysicalqueues/64durables, audit530.008518ms and combined535.525980ms
under20s. All2,281originalfiles unchanged; originals never reopened. Both complete
archives read back. Full13x200/current-source/24h gates remain open.
[Native proof](scale/tier2-retained-isolation-2026-10-06/native-ten-minute/).
[Copied proof](scale/tier2-retained-isolation-2026-10-06/copied-audit/).

## Original retained fanout restart evidence prepared — 2026-10-06

Original Tier2 fanout restart now preserves the existing barrier's exact suspended
parent/six-child/unfinished-child cut, recovered prefix and final descendants.
Read-only repository reviewer requires19 successful original30-second restart cuts,
all native workload/gates/source/profile/closure, unchanged prefixes and exact
six-child/two-grandchild completions. Valid fixture/19 corrupted-evidence controls
pass; integration compiles. Native ten-minute seed1 and independent copied-store
qualification pending; full13x200/current-source gate remains open.
[Preparation](scale/tier2-retained-fanout-2026-10-06/preparation/).

## Reusable closed Tier2 original-byte verification — 2026-10-06

Repository verifier binds pushed canonical proof/fullarchive/single embedded
manifest/current SDK and process evidence, exact originalfiles and owned process
closure before a fresh copy. Visible task descriptors checked; permission limits
explicitly recorded. Three controls and accepted2,281file isolation fixture pass.
Copied producer opt-in --canonical-proof records verifier/report and binds initial
copy hashes to reviewed manifest. No original stores reopened. Pending fanout
native/copied qualification continues on existing handles; full13x200/24h remain.
[Verification](scale/tier2-closed-originals-review-2026-10-06/).

## Original retained fanout restart seed1 qualified — 2026-10-06

Original normal ten-minute e3399bf PASS638.36s:77batches/2,156terminals/23,793entries,
19verified unfinished six-child cuts, unchanged parent+child prefixes and exact
six terminalchildren/two distinct completedgrandchildren each. Native allsix cells/
latency/integrity/history/drain pass. SDK/source/external/profile/observed closure
bound; full6,355member79,499,469byte/fourpart native proof readback passes. Independent
fresh-copy helpereaa56f9 verifies exactfull retainedreport/histories/allthree physical
queues/64durables, audit2.447008218s/combined2.461618250s under20s. Canonical pre-copy
verification executed/retained before copying; visibleFD permission limits recorded.
All2,281originalfiles unchanged; no originalstore reopen. Full4,007member54,135,850byte/
threepart copiedproof readback passes. Full13x200/current-source/24h gates remain open.
[Native proof](scale/tier2-retained-fanout-2026-10-06/native-ten-minute/).
[Copied proof](scale/tier2-retained-fanout-2026-10-06/copied-audit/).

## Original rolling upgrade physical queue diagnostics live — 2026-10-06

Failed123bc4b canonical archive/log correlation binds sequence494 successful
confirmedACK/delivery4 to subsequent native retainedrawmessage with64zero-pending
consumers. Physical deletion cause remains unconfirmed. New diagnostic snapshots
read each pinned publicNATS /jsz before/afterupgrade and atdrain success/failure,
joined under2s withtimestamps/errors. Original gates/budgets remain unchanged.
Clean661239d original normal ten-minute seed1 actualSDK1610819/profile/legacybinary/
threeinitiallegacy+currentservers/source verified. First upgrade/snapshots pass;
allthree/fullnative/copied qualification pending. Original30s/5m/9m30s cadence
retained; reviewer corrected before terminal. Long handles continue unchanged.
[Preparation](scale/tier2-retained-upgrade-2026-10-06/preparation/).
[Live evidence](scale/tier2-retained-upgrade-2026-10-06/live-initial/).

## Original retained rolling upgrade seed1 qualified — 2026-10-06

Normal ten-minute661239d PASS649.10s:2,016terminals/22,252entries, three actual
legacy2.11.17→2.15.0 transitions at30s/5m/9m30s, allsixcells/latency/history/integrity/
drain. Corrected closed-evidence review binds source/executables/profile/closure/
sevencomplete three-peer physicalsnapshots; allfinalphysicalWF_RUN queueszero.
Initial report variable-shadowing failure/fullproof preserved; corrected report
outside immutablearchive, no native rerun. Full6,338member101,144,198byte/fourpart
native archive readback passes. Separate fresh-copy helper7134e12 actualSDK1677774/
threeexternalNATS/1,698inputs/closure, exactfullreport/history/allthreequeues/
64durables verify; audit523.857828ms/combined4.537360143s under20s. All2,312original
files unchanged; no originalstore reopen. Full4,038member50,541,217byte/twopart copied
archive readback passes. Historical35s mixed-version residual cause unconfirmed;
this full all-upgraded row does not erase it. Full13x200/current-source/24h remain.
[Native proof/corrected review](scale/tier2-retained-upgrade-2026-10-06/native-ten-minute/).
[Copied proof](scale/tier2-retained-upgrade-2026-10-06/copied-audit/).

## Original retained positive server-clock row live — 2026-10-06

Clean835d19e originalnormal ten-minute seed1 actualSDK1715508/threeexternalNATS/
694inputs/2CPU/2GiB verified. Exact retainedtoolchain overlay+60s/actualshiftedbinary
and independent pinnedvarz clocks match node2+60s/neutral peers within original±2s.
Reviewer covers both clockdirections, nineteen original30s observations, exactoverlay/
actualexes/fullnativegates/latencycorrection+allterminals/source/profile/closure.
Threecontrols acceptvalidpatterns and reject24clock+4latency variants. Native
terminal/independentcopy/full13x200 remainpending; long24h/million sources unchanged.
[Preparation](scale/tier2-retained-server-clock-2026-10-06/preparation/).
[Live evidence](scale/tier2-retained-server-clock-2026-10-06/positive-live-initial/).

## Positive server-clock native pass and copied physical evidence correction — 2026-10-06

Original835d19e normal ten-minute seed1 native/review passes with3,136terminals,
34,530entries/allcells/19actual+60s observations; full6,305member104,490,832byte
four-part proof verified. Copied qualification pending. Historical copied helper's
three pinned clients use leader-routed Stream.Info: those records prove logical
queue/64durable drain, not three local physical stores. New helper and reviewer
add distinct pinned /jsz local queue checks under unchanged20-second audit budget.
Legacy API evidence explicitly reports copied physical verification false; native
upgrade physical snapshots remain valid. Four controls reject24 corruptions and
closed upgrade crosscheck passes without NATS/store reopen. Fullmatrix/currentmain/
24h/million physical gates remain open.

## Original positive server-clock seed1 copied physical audit accepted — 2026-10-06

Original835d19e normal ten-minute native + fresh independent copied integrity/full
history/localphysicaldrain now qualifies:3,136terminals/34,530entries. Helper4954561
reads distinct pinned /jsz queueszero/64consumers, integrity805.706728ms/whole
4.822946765s<20s,1,698inputs/2,287unchangedoriginals/actualSDK+threeNATSclosure.
Canonicalprecopy binding/full4,013member64,475,064byte/threepart proof verified.
Legacy pause/isolation/fanout copied fixtures also recheck complete counts/history/
logical drain, explicitly no local physical evidence. No original store opened or
legacy NATS rerun. Negativeclock/full13x200/current-source/24h/million finalphysical
gates remain open.

## Original retained negative server-clock row launch verified — 2026-10-06

Clean0b24a84 normal ten-minute seed1 SDK1802177/694source/profile2CPU/2GiB and
three actualNATS binarypaths verified. Exacttime.go -60wallseconds overlay preserves
monotonic clock. Independent public pinnedvarz measures node2-59.987814811s;
neutral peers within2s. Same original producer/reviewer handles remain active;
native terminal/fullproof/copy/full13x200 gates pending. Positiveclock native/copy
accepted at recordedsource/seed; long24h/million processes unchanged.

## Reusable retained rolling-upgrade physical reviewer accepted — 2026-10-06

Repository reviewer binds closed ten-minute original SDK/source/externalinputs/
producer/profile/nativegates, exact2.11.17/current2.15.0 binaries/paths and one
initial/replacement generation pernode. Three upgrades retain270scadence/version
vectors/monitorcuts/seven pinned physical snapshots/finaldrain/64durables. Accepted
661239d seed1 rechecks2,016terminals/22,252entries. Three controls reject27fault/
physical corruptions. Copied reviewer fixes submicrosecond ordering; reversed1ns
control rejects and positiveclock copied evidence rechecks. No NATS process/store
reopen; historicalfailedupgrade/full13x200/current-main/24h/million gates remain.

## Negative clock native pass and fanout/projection physical scope correction — 2026-10-06

Original0b24a84 normal ten-minute negativeclock seed1 native/review passes3,332
terminals/36,768entries/allcells/19confirmed-60s observations. Full6,305member/
105,596,417byte/fivepart proof verified; independentcopy pending. Fanout/projection
copied and500-child native drain helper records are three leader-routed API views,
not all-three local physical stores. Historical retaineddata/results/logicaldrain
outcomes remain; stronger physical claims need new witnesses. Copied templates now
add publicJsz localserver snapshots under unchanged20s, bothcompile. Freshruntime/
500-child native physical proof/fullmatrix/current-source/24h gates remain open.

## Negativeclock copied accepted; full500 local physical repair prepared — 2026-10-06

Original0b24a84 ten-minute negativeclock seed1 native+copied now qualifies full
3,332terminals/36,768entries/history/localphysicaldrain/64durables. Copied audit
635.639039ms/combined4.652001527s<20s;1,698inputs/2,287unchangedoriginals/canonical
precopy/executableclosure/full4,013member65,914,668byte/threepart proof verified.
Full500-child native drain now adds actualServer.Jsz localstates/IDs/intervals within
original5-minute case deadline, retains failures, and requires local_monitors=3.
Fourteen controls/integrationcompile pass; reusable sixcase independent reviewer
prepared. Fullsix native/copy localphysical qualification pending; fullmatrix/
current-source/24h/million gates remain open. No original long handle restarted.

## Fullsix fanout physical failure; bounded constructor retry prepared — 2026-10-06

Originalaa2015a fullsix raceFAIL370.79s; five localphysicaldrains recheck, results/first
fails40.84s at constructor signal-stream metadata, beforedrain. Complete16,793member/
65,523,988byte/threepart failedproof preserved; SDK/source694/external3286/closure
verified. Metadata timeoutcause unconfirmed. Worker.New has5s startupattempt and
expects callerretry; originalcase retains5min. Fanout callers now retry only typed
transients within originalcase; permanent/cancel errors failfast, allattempts retained,
late success rejected/unusedworkerclosed. Productionruntimeunchanged. Three race
controls pass. Fullsix fresh500-child samecuts/prefix/results/localdrain requalification
prepared; no originalstore reopened/longhandle restart/fullmatrix or24h promotion.

## Full six fanout native and copied local physical qualification — 2026-10-06

The fresh full R3/file/race/2CPU/2GiB campaign at `cdd4c7b` passes all six
original 500-child parent SIGKILL plus library journal-leader restart cuts in
525.46 s. Each preserves the exact pre-kill prefix, all 500 child results, parent
249500 and 501 complete invocation/journal/terminal records. Actual local public
Jsz observations on three distinct servers show WF_RUN zero messages/64 consumers,
with every durable zero pending/ack-pending after all 64 drain workers joined,
within each unchanged five-minute case deadline. Actual SDK/source/module/external
inputs and closure independently verified; complete 16,805-member native proof
retained. [Native qualification](scale/fanout-combined-local-physical-2026-10-06/native-retry/).

All six fresh copied audits at helper `a4e9016` also qualify complete cohorts,
results, all three local physical states, durable census and unchanged originals.
Each complete review is below the original 20-second budget. Full copied archives
retain 23,931 members/157,628,217 bytes/nine parts. Stronger read-only review rechecks
all six and rejects boolean durations and omitted build inputs; originals are
never reopened. [Copied qualification and controls](scale/fanout-combined-local-physical-2026-10-06/copied-local-physical/).

All eighteen native worker starts succeeded on their first attempt; this pass
supports the full native case verdict but does not demonstrate transient retry
recovery. The prior failed full campaign and unconfirmed constructor timeout cause
remain preserved. Other combined fault cases, final-source full matrices, original
million-timer final physical drain and actual 24-hour gates remain open.

## Bulk final latency audit preparation — 2026-10-06

The existing point latency rules now share one evaluator for complete logical
journals and raw retained timestamps. Point adapters continue using fresh queries;
production runtime is unchanged. Three race control groups verify exact causal
samples, signed clock normalization, impossible/missing evidence and cancellation
without partial success. The old `36f82cf` point implementation is retained
independently and its renamed body is byte-identical to that source.

Fresh R3 race native160-real-workflow run at `14efcfb` passes47.89s with every
sample equal across frozen legacy/shared/serial/parallel/cached adapters and the
existing deadline controls rejecting. Actual SDK/captured699Git source inputs/
closure and full2,803-member proof independently verify. External compiler input
capture is not exhaustive. [Native reducer equivalence](scale/bulk-final-latency-preparation-2026-10-06/native-reducer-oracle/).

Bulk acquisition, bounded memory, snapshot handling and large-cohort/fault
qualification remain pending. This refactor does not qualify the original
six-minute final audit at 24-hour population sizes or change20s point/20s per
retained attempt/60s aggregate limits. The original live24h source remains unchanged.

## Native bulk timestamp sample equivalence — 2026-10-06

The explicit chunked retained walker now supplies a bulk latency acquisition
candidate. It captures and checks four complete raw source cuts/censuses, projects
causal journal fields, accounts conservatively for retained projections/output,
uses the existing point path for snapshots, and rejects changed cuts/cursors,
errors and canceled/incomplete samples. The existing full integrity audit remains
required independently. No matrix/soak launcher selects bulk yet.

Initial5fa0926 full160 native comparison failed despite complete scans; full failed
proof retained. Delivery metadata constructs local Time.Unix representations while
point JSON decodes UTC. An equal-instant/distinct-location control reproduces that
representation gap; bulk now normalizes UTC. The old failure did not save individual
mismatches, so its exact first mismatch is not independently established. New
fixtures retain comparison status and first mismatch/Go representations.

Fresh7bd5571 R3 race160-real-workflow campaign PASS26.82s: allsamples exactly equal
bulk/frozenlegacy/shared/serial/parallel/cached, independent integrity/census/source/
consumer stability and deadline rejection pass. ActualSDK/703Gitinputs/closure and
complete archive verified; external compiler inputs notexhaustivelycaptured.
[Corrected native proof](scale/bulk-final-latency-preparation-2026-10-06/native-bulk-oracle-v2/).

Large real-cohort throughput/process memory, snapshot fallback, protobuf/purge/reuse
and native fault recovery remain pending before adoption. Conservative charged-data
limit is not an RSS guarantee. Original20/60-second retained audit,20-second point
fallback and six-minute final-stage limits remain; original long handles unchanged.

### Bulk latency snapshot and encoding qualification

Run the retained native 160-workflow oracle with `WF_MATRIX_LATENCY_ENCODING=json`
and `protobuf-v1`, plus `WF_MATRIX_BULK_SNAPSHOT_CONTROLS=1`. Actual worker writes
must match the frozen original point samples. Write snapshot manifests before
purge for all workflows; independently recheck complete integrity and verify all
160 bulk snapshot fallbacks equal the original samples. Purge one covered prefix,
recheck logical integrity, and require both point and bulk latency paths to reject
missing server timestamps without partial samples. Compacted journals cannot prove
original latency samples unless original timestamps are independently retained.
These controls do not replace large-cohort, memory, transport-fault or 24-hour gates.

### Bulk latency qualification on a real retained invocation prefix

Preserve the full quiet store copies and source censuses; do not truncate stores
to fit a cohort. Verify the inclusive invocation cutoff and complete bounded
integrity independently under the original20s budget. Bulk acquisition must scan
complete source cuts, identify excluded subjects from INV, retain their last
journal times for external child dependencies, and reject unknown subjects or
source changes. A97-invocation native prefix includes the first parent while its
child lies beyond the cutoff; its samples must exactly match the frozen point
oracle. Run87920 real-workflow bulk mode (`WF_MATRIX_BULK_LATENCY_COHORT=1`) against
a canonical-bound accepted point oracle (`WF_MATRIX_LATENCY_COHORT_ORACLE`),
retaining the identical original completion deadline and6m final audit budget.
No partial samples qualify. Full400k/memory/fault/default/24h gates still apply.

### Real-cohort state admission diagnosis before bulk adoption

The4d03dd1 real87920 attempt failed its independent original20s fresh-state check
before bulk latency acquisition (58,403 initial records/revision96,250, no initial
completion barrier). Preserve that verdict and complete closed originals. Instrument
state-watch creation, initial pending count, received/included revisions, completion
barrier, cleanup and journal-phase timing on a new verified disposable copy. Compare
fresh-state acquisition alone and concurrent journal/state acquisition with their
complete source inventories and original budgets. No cached outcome values, longer
release budget, reopened failed fixture or assumed NATS/runtime cause can qualify
this gate. A faster state transport must independently preserve complete KV latest
values, deletion/purge semantics, snapshot/terminal validation and cursor cleanup.

### Initial-state per-call versus whole-stream deadline observation

Use `integrity.WithStateSnapshotObserver` to record actual state-watch attempt
budgets and distinguish creation, delivered prefix, initial completion and stop.
The complete watch currently sits inside the generic2s/3-attempt API wrapper.
On fresh verified complete copies, run standalone20s state collection followed by
existing concurrent87920 full admission; retain consumer creation metadata, source
censuses, every attempt's deadline/counters and cleanup. Each phase has its own
verdict. This ordered diagnostic is not an isolated speed ratio or a release pass.
Keep original20s/60s/3 retained-audit and6m final-latency gates; qualify any deadline
scope correction against exact initial-set/terminal/snapshot and fault controls.

### Fresh-state stream deadline scope correction

An initial-state snapshot is a whole retained stream read. For bounded audits,
use the caller's existing deadline for the complete initial set, preserve at most
three transient attempts with canceled/closed contexts, and discard incomplete
maps. Keep individual metadata/record requests at2s and retain the old finite
wrapper for unbounded callers. Deterministic delivery-window and retry/cancel
controls must distinguish this from the former2s whole-set bound. Qualify changed
state behavior on fresh real-cohort copies with complete source counts, exact
terminal/snapshot checks, cleanup, and original20s/60s/3 budgets; then complete
87920 bulk/frozen-point comparison under the unchanged6m final stage. Ordered
state diagnostic results alone cannot qualify bulk or release gates.

### Corrected state deadline and real87920 bulk comparison accepted — 2026-10-06

At65c9036 fresh normal R5 nativePASS53.91s. Original20s integrity checks before
and after pass87,920 invocations /969,925 entries /87,920 terminals; all latency
samples exactly equal the complete canonical accepted point oracle with identical
original completion deadline. Bulk stage16.775331767s is within original6m.
Complete source counts/cuts/consumer cleanup, unchanged donor hashes and actual
SDK/five server closure independently verified. Complete4,434-file base/delta
proof verified. This qualifies the quiet real87920 cohort at recorded source;
full400k/memory/fault/default/currentmatrix/24h gates remain required. Bulk is not
adopted in live campaigns. Earlier failures remain preserved without attributed
cause. [Evidence](scale/state-admission-deadlines-2026-10-06/native-bulk-whole-state/).

### Bulk cursor-owner recovery and valid capacity fixture preparation

On a fresh archive-verified87920 copy, remove only recorded historical audit
cursors before source cuts, then run independent20s full integrity. During the6m
bulk stage, target actual R1 memory AckNone journal cursor metadata, confirm its
physical owner SIGKILL and same-store restart, require a distinct later cursor
start and complete source/consumer cleanup, and compare every sample to the
canonical accepted original point oracle. Repeat independent20s full integrity.
Preserve preparation, fault, actual server executable/mount observations and the
complete closed fixture. No partial results or changed deadlines qualify.

The original synthetic400k retained-read donor's empty StepRequested payloads
are invalid for the original point latency evaluator. Future capacity preparation
writes valid activity request JSON while preserving400k/4.8M counts. Earlier
retained-read qualification remains unchanged in scope; full400k valid latency
and RSS qualification still require native evidence.

### Disk-pressure terminal campaigns and archive storage — 2026-10-06

Original journal24h row is terminal at6h48m with replica-readiness failure followed
by no-space evidence/archive errors. Million wrapper is terminal with no-space
source-after failure; its stale921,929-receipt report is not a final delivery or
physical-drain pass. Preserve all current files, including partial artifacts,
without restarting old handles or reconstructing missing original ledgers as if
they were producer evidence. Complete closed archives now reside in S3 after full
GET byte readback; complete file inventories, archive hashes and receipts are in
Git. No archive-part duplicates are needed for this format. Original primary
stores/source/exes remain local. Future long/native runs must reserve space for
remaining campaign growth, copy allocation and final archival across concurrent
jobs; a short-copy-only reserve cannot qualify safe campaign admission.
[Terminal evidence and full proofs](scale/terminal-campaigns-disk-pressure-2026-10-06/).

### Real87920 bulk cursor-owner restart accepted — 2026-10-06

At2621015 fresh copied normal R5 nativePASS49.44s: active R1 memory AckNone JRN
cursor owner actualSIGKILL and same-store restart confirmed, distinct replacement
start24738, complete source counts and zero-consumer cleanup. Original20s integrity
before/after both pass87,920/969,925/87,920. Every sample exactly equals complete
canonical original point oracle;18.143767852s under original6m. SDK/six actual server
incarnations closed and original donor unchanged. Recorded-source quiet real-cohort
fault qualification does not replace full400k valid latency/RSS, other faults,
current/default matrices or24h/million release gates. Bulk remains opt-in.
[Evidence](scale/bulk-cursor-fault-preparation-2026-10-06/native-real87920-owner-restart-v2/).

### Closed copied block-media reclamation and rejected-proof reporting — 2026-10-06

Two disposable copied block-media audit trees are reclaimed only after complete
committed archive/member/current-file verification, detached-image ledgers,
closed SDK/server executable checks, current mount/loop/Docker checks and visible
descriptor checks. Recover1,179,799,552 allocated bytes; original donors,
source/executable/metadata, caches and canonical archives retained. About3.8GB free
still falls below the16GiB long-campaign launch baseline plus overlapping reserves.
[Recovery](scale/closed-blockdisk-copy-recovery-2026-10-06/).

The real87920 proof writer discards all samples and suppresses its oracle-pass
flag on final full-integrity errors or unexpected counts. Integration compilation
passes; this reporting correction adds no new native qualification. Next capacity
work requires a fresh valid full400k latency/memory fixture, with the original
20s full integrity and6m latency limits retained.

### Fresh full400k latency/memory gate prepared — 2026-10-06

`TestMatrixBulkLatencyFull400kCapacity` creates an original fresh R5 file-backed
400,000-invocation/4.8M-entry activity population. INV acknowledgment precedes JRN
publication across stream leaders. Fixed population has no reduced-count override.
Original20s complete integrity checks run before and after bulk; bulk computation
and comparison retain the original6m deadline. A separate full public timestamp
walk checks the entire known input shape and derives800,000 expected start/terminal
samples without calling the bulk projector or shared reducer.256 spread-out
workflows, including both endpoints, also match the independent frozen point
oracle. Every bulk sample must equal its independent expected sample; complete
accepted samples are persisted only after final integrity. This synthetic gate
does not replace real-workflow, other cursor faults, default/fullmatrix or24h gates.

Linux RUSAGE_SELF records whole SDK lifetime peak RSS, including preparation and
oracle work but excluding Docker servers. Native producer pins4CPU/GOGC500/4GiB
and observes actual SDK/server executables, mounts, source before/after and public
process memory.5GiB minimum free is checked on the fixture filesystem before any
root/checkout creation; this is a finite fixture launch estimate, not an ongoing
reservation or a24h admission. Compilation/skip and invalid-input race controls
pass; native qualification is still pending. Additional23 canonical raw duplicate
archives verified and reclaimed1,084,948,480 allocated bytes; about4.8GB free still
falls below launch admission. Original primary media, sources/exes/caches and Git
proof retained. [Recovery](scale/remaining-raw-proof-duplicate-recovery-2026-10-06/).

### Fresh valid full400k bulk latency capacity accepted — 2026-10-06

At86a9a33 fresh R5 full400k/4.8M valid activity population passes native346.87s.
Original20s complete integrity checks pass17.935866136s before/16.185203628s after.
Original6m bulk read/reduction and comparison finish19.345814968s. Every one of
800,000 samples equals the complete independent timestamp/input-shape oracle;
256 distributed frozen point-oracle witnesses pass. Complete source counts and
zero-consumer cleanup verified, application charge1,581,600,000 bytes. Source
unchanged; actual SDK3147263 and five2.15 server processes closed. Independent
artifact review verifies all persisted sample coordinates/census/timestamp math.
Complete1,793-file fixture/1,794-member archive637,549,119 bytes fully read back;
all current original bytes/modes/mtimes unchanged. Complete archive/metadata/inventory
pass full S3 GET hash/size readback. Synthetic capacity
does not qualify real-workflow/fault/default/currentmatrix/24h adoption of bulk.
[Evidence](scale/full400k-bulk-capacity-preparation-2026-10-06/native-valid-population/).

Native RUSAGE_SELF3,804,444KiB (3.63GiB) is SDK highwater measured through test-body
completion, before cleanup/exit and excluding Docker servers. Earlier field name
suggested a final lifetime peak; independent review explicitly corrects that scope.
Future test field is named for test-body highwater and the producer now records
exact-child wait4 RSS through SDK exit. Real OS control preserves nonzero exit17
and excludes a larger prior subprocess. It does not retroactively provide final
RSS for this accepted run. Original counts/deadlines and prior verdicts unchanged.

### Sustained bulk final latency entry point prepared — 2026-10-06

R5 producer/harness expose explicit `--bulk-final-latency`, with optional
`--compare-bulk-point` for complete original-point equivalence in the same6m
stage.4GiB/4CPU/GOGC500 profile required. Bulk admission uses a complete retained
integrity report; post-stage complete report must equal it, including entry counts,
or all samples are discarded. Full source census/cuts, original5m completion
deadline, p99/recovery/history/drain/fault/checkpoint gates retained. Server-clock
controller, wire-rollout and cached-point profile conflicts are rejected; comparison
requires bulk selection. Defaults unchanged. Thirteen producer controls and Go
compile/skip pass. First native qualification must use the full original ten-minute
journal fault row and compare every sample, rather than promote a short smoke.

### First sustained bulk row fails strict source stability — 2026-10-06

Original10m journal seed1 at29904c5 fails native620.54s. Before full integrity
passes2,604 invocations/journals/terminals and28,689 entries; bulk stage rejects
a KV_WF_STATE source-cut change after2.241896878s. No samples accepted and no
point-equivalence/row/default/fullmatrix/24h promotion. Actual SDK3166840 and24
observed NATS2.15 incarnations closed; source unchanged. Complete5,502-file
fixture/5,503-member archive141,763,935 bytes verified; complete archive,
metadata and inventory pass full S3 GET hash/size readback.

Original error does not identify whether data coordinates/counts or consumer
cleanup changed, so a server-side cause is unconfirmed. Future bulk stats capture
all four final source cuts, and errors show exact before/after fields with the
original equality gate retained. Projection/delivery race controls pass1.089s.
Next: a fresh bounded diagnostic to distinguish actor/state writes from cleanup
and metadata behavior, then rerun full original ten-minute qualification after a
justified correction. No old original/failed fixture is reopened.

### Corrected original ten-minute bulk final audit accepted — 2026-10-06

The fresh35s diagnostic at009de76 identifies three repair loops writing scan
cursors in WF_STATE during the final bulk snapshot, even after all terminal
results exist. Each repair writer now stops and joins before explicit bulk
collection; workers and dispatch remain live for drain. Source equality gates
and original audit/completion/p99/history/fault/checkpoint deadlines remain.
Seeded production-loop/shared-KV regression passes race and exact replays.
Fresh original10m journal seed1 atca7a1fa passes626.35s:93cohorts,2,604 complete
invocations/journals/terminals,28,693 entries,19 actual journal-leader faults and
all nine required checkpoint receipts. Before/after integrity equals; four source
cuts stable, consumers0. Bulk plus every-point comparison passes8.697232942s under
original6m; actual source/SDK/profile and all24 observed server incarnations closed
independently checked. Full canonical S3 proof verified. This accepts the explicit
original ten-minute component at executed source; default/other rows/current full
matrix and actual24h remain open.
[Qualification](scale/sustained-bulk-final-latency-2026-10-06/joined-journal-ten-minute/).

### Outer handler cancellation and delivery handoff qualification — 2026-10-06

Ordinary handlers and named continuations run behind an outer cancellation
boundary. Error/panic/Goexit are retained; cancellation wins when completion is
also ready. Abandoned user code owns its buffers, which the canceled delivery
does not inspect; late SDK calls reject canceled context. Shutdown, lease loss
and durable cancellation can return delivery ownership while ignored user code
remains outside SDK control. External effects still require idempotency.
Focused1,000-seed production-worker handoffs and native R3 worker-stop/durable-cancel
controls pass. New successor epoch, exact result/cancel outcome, unchanged accepted
journal, rejected late SDK call and three distinct public local Jsz drain witnesses
are checked within original contexts. Outer Goexit retains bounded three-attempt
terminal policy. Three cancellation pins changed cleanup/redelivery ordering;
all scheduler choices remain identical, prior bytes/diffs preserved, two independent
replacement processes agree, and all392 pins pass. Complete122-workload1,000-seed
race qualification is live at3e35e7c with original60m budget and complete source,
binary, seed and pin inventories. Current100k normal and broader native fault/matrix
qualification remain open; focused results do not establish full-source acceptance.
[Controls and transition](scale/outer-handler-cancellation-2026-10-06/pin-transition/).
[Live qualifier](scale/outer-handler-cancellation-2026-10-06/full-race-launch/).

### Closed failed media storage policy — 2026-10-06

User-provided S3 supports offloading closed failed primary store media. Require
complete canonical archive/inventory metadata committed and pushed first, full
S3 compressed-body/hash/size and every-member readback, exact current full file
census, and repeated producer/process/mount/visible-descriptor closure with limits
recorded. Remove only selected store directories after those checks; retain source,
executables, configs, logs, partial artifacts and metadata locally and verify all
remaining bytes/modes/mtimes. Future audits restore fresh verified copies. Offload
changes storage locality, never the original failed verdict or a release gate.
Accepted capacity donors remain local while needed. The actual24h admission floor
of16GiB plus concurrent growth/copy/archive headroom remains unchanged.
[First media offload](scale/terminal-campaigns-disk-pressure-2026-10-06/journal-store-media-offload/).
[Second media offload](scale/watch-observed-journal-24h-2026-10-05/store-media-offload-2026-10-06/).

### Full corrected outer-handler Tier1 race qualification accepted — 2026-10-06

At executed3e35e7c, full original1,000-seed race qualification passes1856.59s
under unchanged60m: all122 workloads complete1–1,000 (122,000 bodies),392 pins,
179 top-level passes and only two prescribed trace-replay/minimize skips. Independent
complete checker matches retained result; all1,793 captured source files exactly
match Git/before/after/current inventory, actual SDK binary/profile match retained
launch evidence, producer/SDK/supervisor closed. Complete archived fixture members
and unchanged current census verified. Aggregate126,947 generated schedules and
27,815,980 transport events are accounted for. Current100k-normal qualification
and broader native fault combinations/current full real matrices/million physical
drain/actual24h remain open. NATS Raft/disk internals are outside model scope.
[Terminal review](scale/outer-handler-cancellation-2026-10-06/full-race-terminal/).

### Complete current100k normal Tier1 launch verified — 2026-10-06

Original full100,000-seed normal qualifier is live at4f93039, isolated source,
original300m/GOMAXPROCS2/GOMEMLIMIT512MiB,122 seeded workloads and392 pins.
Actual SDK3312214 non-race hash/argv/profile verified, all1,803 captured source
inputs exactly bind to Git and clean detached checkout. Runtime Go/dependencies/
sim testdata equal accepted race3e35e7c. Complete12.2M-body seed checker, terminal
native verdict, unchanged after-source and SDK/producer closure remain required;
no current-normal/full real-matrix/24h promotion from a live launch. Accepted race
proof has canonical S3 full readback.
[Normal launch](scale/outer-handler-cancellation-2026-10-06/full-normal100k-launch/).

### Original bulk consumer-leader ten-minute qualification live — 2026-10-06

Fresh original10m consumer-leader row seed1 atb861f95 now runs with explicit
chunked retained integrity/bulk final latency and full point comparison. Actual
SDK3316473/profile/build and all1,805 source files independently verified, five
NATS2.15 executable identities observed; faults select confirmed active consumer
ACK-pending work. Periodic observer retains actual SDK/server incarnations; shared
CPU with the100k normal qualifier and cgroup quota recorded. Original audit/final/
completion/p99/history/checkpoint/drain gates retained. Full terminal row/artifact/
source-after/closure and complete canonical proof remain mandatory. No live-launch
row/default/fullmatrix/24h promotion.
[Launch](scale/sustained-bulk-final-latency-2026-10-06/joined-consumer-ten-minute-launch/).

### Original bulk consumer-leader ten-minute component accepted — 2026-10-06

At executedb861f95 fresh original10m consumer-leader seed1 passes646.15s:
2,604invocations/journals/terminals,28,712 entries,19 confirmed active-delivery
leader faults, nine required checkpoint receipts. Four source cuts stable with
audit consumers0, full integrity before/after equal. Explicit bulk and all12,276
point samples agree11.982354808s under unchanged6m. Original20s/60s, completion/
p99/history/fault/checkpoint/drain gates pass. Independent source/actual SDK/profile/
24 observed server incarnation closure and whole5,572-member147,177,686-byte
fixture proof verified; supplemental full current census/compressed/member/global
visible process/mount/descriptor closure passes after separate invocation avoids
matching the creating shell's fixture literal. Shared CPU with100k normal recorded.
This qualifies explicit original consumer component only; default/other rows/full
current real matrices and actual24h remain open.
[Terminal proof](scale/sustained-bulk-final-latency-2026-10-06/joined-consumer-ten-minute/).

### Original bulk all-server restart ten-minute row live — 2026-10-06

At executede09442a fresh original10m all-server SIGKILL/restart seed1 runs with
explicit chunked retained integrity/bulk/full point comparison. Actual SDK3375975
profile/build,1,814 source inputs and five NATS executable identities independently
verified; first two operation receipts prove all five removals precede any restart.
Shared CPU with100k normal qualifier/cgroup quota recorded. Original audit/final/
completion/p99/history/checkpoint/drain and full all-fault operation gates retained.
Terminal/source-after/full row-artifact checks/closure/canonical proof remain
mandatory; no live-launch qualification or default/fullmatrix/24h promotion.
[Launch](scale/sustained-bulk-final-latency-2026-10-06/joined-restart-ten-minute-launch/).

### Original all-server retry failure preserved and localized — 2026-10-06

Original10m all-server row seed1 ate09442a fails611.10s before bulk final audit:
original30s client retry ends with503. Two matrixsignal batch61 signal-5 histories
show25 attempts each across29.940s with uncertain/not-published outcomes, no
signal-6/7. Fault18 requires35.205s heal (29.848s after last actual restart), next
kill begins0.173s afterheal. These timings and NO-quorum logs establish observed
unavailability/retry exhaustion, not server/root CPU/disk causality or lost data.
Source/actual SDK/profile/100 observed server incarnations closed; full5,421-member/
152,988,357-byte fixture/current-census/compressed/member/global visible closure
proof verified. Missing final bulk artifact explicitly retained, initial missing-
artifact reviewer failure and corrected scope preserved. No final comparison/full
integrity/row promotion. Original budgets/cadence/p99 remain unchanged. Native harness
error messages now identify operation/workflow/signal index/idempotency key while
preserving wrapped error; compile/skip passes. Current100k normal continues from
unchanged isolated source. Next diagnosis must use retained timelines/logs and
fresh controlled cases with an explicit hypothesis; no blind campaign rerun.
[Corrected failed proof](scale/sustained-bulk-final-latency-2026-10-06/joined-restart-ten-minute/corrected-independent-review.json).

## Native outer-handler lease-expiry combination prepared — 2026-10-06

`TestOuterHandlerNativeLeaseExpiryAndLateAppend` covers ordinary handlers and
named continuation stages against a real R3 file-backed cluster. It withholds
only the old owner's heartbeat ticks, verifies the production12s KV TTL and
server-observed expiry, then starts a live successor on the unchanged13s
AckWait consumer. The successor must complete within30s, in a higher epoch.
Resuming the old heartbeat while its handler remains held must produce explicit
`lease_heartbeat_lost`, bounded worker shutdown, unchanged accepted journal and
no late effect. All three public local Jsz queues must physically drain.

Optional `WF_OUTER_HANDLER_STORE_ROOT` retains fresh per-test stores for exact
archive/provenance review; it does not alter the test's deadlines or assertions.
The test compiles. Native race execution and independent evidence review are
pending; full combined and release qualification remain open.

### Native lease-expiry observation guard correction

Inspection of `lease.Lease.renew` shows the production heartbeat reports wrapped
`ErrLost` with its underlying real JetStream CAS error. The initial native
control wrongly required the bare error string. Its original isolated run stays
at13af4d2; the corrected assertion preserves reason/owner/epoch checks and
requires the lost-lease prefix plus underlying cause. This changes observation
only, with no runtime, TTL, AckWait, counts or deadlines changed. Original
output/stores must be retained before a fresh corrected-source qualification.

## Focused native lease-expiry combinations accepted — 2026-10-06

Corrected ordinary-handler and named-continuation cases atfb1e515 pass race
35.32s, with productionTTL12s/AckWait13s takeovers13.0728/13.0774s. Independent
source/actual-SDK/raw-config/closure and complete archive checks pass; full S3
readback preserves all original files. Late SDK calls cause no effect or journal
change after successor completion and old heartbeat fencing. The original failed
wrapped-error observer guard remains separately preserved. No production change
or deadline relaxation. [Accepted component and limits](scale/outer-handler-lease-expiry-2026-10-06/).
Broader combined cut, current fullmatrix, million physical-drain and24h gates remain.

## Original24h terminal reviewer verified and waiting — 2026-10-06

The pinned terminal reviewer now waits on the same original producer/observer
units. Complete original24h/native24h20m deadline and production2m sync,
source/SDK/profile/known process incarnation closure, legacy member hashes,
original checkpoint/count/history/p99/drain and bulk/every-point6m report
regeneration are mandatory. PID reuse is distinguished from a live original
incarnation; observation timeout alone is never a native verdict or restart.

Full read-only validation passes on a verified fresh S3 restore of accepted
original10m sourceca7a1fa: all5,633 file bytes/modes/mtimes unchanged, complete
compiled-source/current row/event/fencing reports regenerated. Promotion of that
evidence to24h is rejected. Three control groups reject incompatible profiles,
missing gates/admission, live processes and shortened/over-deadline terminals.
Disposable restore/observer/archive staging was removed after exact census and
visible closure; canonical S3 originals unchanged.

The watcher will preserve a terminal rejection as unqualified current closed
files; canonical metadata/full S3 readback remain separate required steps.
[Preparation, controls and pinned source](scale/bulk-journal-24h-2026-10-06/terminal-preparation/).
Actual24h, other rows/current fullmatrix/default adoption remain open.


## Domain retirement across native all-server SIGKILL — 2026-10-06

Focused `TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndAllServerSIGKILL` is qualified at executed4b4719e: race body32.34s / SDK33.42s. Three real2.15 server processes admit `WFRETIRE` on pinned clients, all are reaped with SIGKILL at the fresh-manifest reply-loss cut, and three replacement PID/server identities confirm the domain through actual connection and AccountInfo by6.611s under the original30s whole-cut gate. Existing startup30s/scenario60s/production leases remain unchanged.

Strict retirement/reuse verifies generation1→3, two objects reclaimed while survivor/shared references remain, old manifest rejection, exactly three effects/two terminals and all-peer fresh references/integrity/GC checks. Two fresh handler entries exercise one controlled manifest loss. Independent review binds1871 Git/before/after/current inputs, actual SDK race binary/profile, six observed real server incarnations, visible closure and all2324 complete archive members. No original store was reopened; sampled process observation is not exhaustive. [Complete proof](scale/continuation-domain-all-server-sigkill-2026-10-06/).

This closes the focused domain manifest-loss/all-server SIGKILL component. Other domain/lease-expiry cuts, forced absence confirmation, legacy versions, active-writer GC, full current-source matrices and actual24h remain open. The normal100k simulation and original24h journal handles are still active, without terminal acceptance. No unchanged Tier1 graph rerun is required for this integration-only fixture.


## Real-domain object absence and snapshot leader controls — 2026-10-06

At executed67e8586, both result-read and compacted-snapshot controls now run against actual R3 `WFRESULT` servers using the public domain constructor. Every peer confirms actual connection/AccountInfo domain. Weak missing-object/old-manifest responses are synthetic client-boundary controls; administrative leader confirmations, stored payloads and deletion/corruption are real.

All four default/domain suites and26 subcases pass under race (4.78/4.95/7.72/7.38s, actual SDK25.90s). Trace requires `$JS.WFRESULT.API.STREAM.MSG.GET.OBJ_WF_BLOB` for worker/client recovery and the domain state leader route for exact200-record/revision2 reconstruction, with zero direct manifest reads. Real deletion/hash/malformed metadata and blocked oracle deadline/cancellation checks retain original targets. Default controls also pass after the shared fixture refactor.

Independent review binds1872 Git/before/after/current source files, actual SDK race binary/profile, twelve closed R3 journal configurations and all3328 complete archive members. [Complete native proof](scale/domain-object-absence-2026-10-06/) preserves current original stores without reopening them. This qualifies forced weak-absence handling on a real domain for result/snapshot adapters. Natural follower lag, combined retirement/forced-absence/server cuts, legacy versions, active-writer GC, full current-source matrices and actual24h remain open. Existing long-run handles continue unchanged; no terminal acceptance is inferred.


## Persistent domain CI and domain lease expiry SIGKILL — 2026-10-06

Dedicated `domain-runtime-controls` push/PR/manual coverage now prevents the opt-in result/snapshot controls from silently skipping. It records clean selected Git source, actual race SDK executable/birth/arguments/profile, closed raw stores and complete verified archives; missing/duplicate/skipped/wrong-route coverage cannot qualify. Two matrix rows run four default/domain read suites plus ordinary domain retirement and the new all-server SIGKILL/production lease-expiry combination. Twelve actual original/replacement native server incarnations are required for the retirement pair. Four parser guard groups pass, including slow-heal/epoch-regression/missing-kill negatives. Triggers include production SDK/identity/integrity/reconcile changes.

Exact CLI native qualification at1558ffc passes read27.50s and retirement81.05s. Ordinary body32.88s; expiry body47.02s holds all servers down13.000s against production12s TTL, then confirms all-peer domain metadata by18.448s from whole-cut start and terminal epoch76 above54. Strict generation1→3, two collected objects, shared/survivor/fresh references, one manifest reply loss, exact three effects/two terminals remain. Original30s startup/whole-cut and60s scenario targets were preserved.

Independent review binds both source copies/Git/before/after, actual SDKs and all complete archive members without reopening stores. [Complete proof](scale/domain-runtime-ci-2026-10-06/). Hosted37484576057 at1558ffc is queued, so hosted acceptance remains unproven. This closes the focused real-domain production lease-expiry/all-server SIGKILL component and persistent opt-in coverage gap. Other combined domain cuts, legacy versions, active-writer GC, full current-source matrices and actual24h remain open. Original normal100k and actual24h handles continue active.


## Fresh complete Tier2 partition range queued — 2026-10-06

Prepared and dispatched[37486948256](https://github.com/AntPAllen/js-wf/actions/runs/37486948256) atfcf2e94: partition seeds1–200, each original10m, with unchanged18m SDK/25m job and original audit/history/latency/drain requirements. Local24 matrix parser and one workload-source preflight tests pass; the planner covers exactly200 singleton jobs. No row/native acceptance is claimed while queued.

The completed old37149506857 job111287268311 atc4fed shows seed1 failing batch50/cutoff1400 on terminal state missing through weak KV Get. Complete raw job log/source comparison is preserved. Current default audits ask the state stream leader with caller context; this is a concrete relevant reader-path change, not confirmation of historical server-side cause or blanket reclassification of all seventeen failed shards. Earlier full campaign remains queued/running for other rows, unchanged.

A read-only user-unit observer pins the new run/source and records statuses, then collects available full terminal logs, raw artifacts and source-selection evidence into a complete verified archive. Collection does not qualify the row; missing/failed jobs or artifacts remain failures, and exact source/coverage/fault/history/latency/native-duration review remains required. [Preparation and launch](scale/current-tier2-partition-2026-10-06/). Original normal100k and actual24h handles remain live; the simulation moved past its long result-budget body without restart. Full13-row/current-source and actual24h release gates stay open.


## Independent partition raw-evidence reviewer validated — 2026-10-06

Extended the existing Tier2 shard reviewer to the partition row and explicitly selected singleton/sharded job layouts. It requires actual row/source/artifact/range/duration binding, exact minority route counts[4,4,0] and acknowledged majority writes, all nineteen30s faults and the original35s cut budget. Every raw latency sample and all per-type summary counts/p99/progress/deadlines are recomputed; all three independent history models retain recorded-source dependency checks. Timestamp parsing is an independent strict RFC3339Nano primitive rather than a dependency on an unrelated R5 helper.

Eight parser/binding/control tests pass. A fresh actual provider ZIP/digest/member readback of historical82a7d6c partition seed1 fully passes:1568 invocations/17307 entries/19 faults/7392 samples/2016 operations, three whole-history models Ok. Three disposable real-data mutations reject missing majority commit,1ns delay mismatch and a second successful Start; original bytes unchanged. Complete control/model/provenance archive preserved. [Proof and usage](scale/current-tier2-partition-2026-10-06/).

This validates independent review for the new200-seed queued run37486948256; the new row is still unqualified, with only its generator job queued at observation. No native rerun or store opening occurred. Historical selected-seed scope only; no parent/fullcurrentmatrix/24h acceptance or unavailable native SDK/store proof is claimed. Original normal100k and actual24h handles remain active.


## Complete outer-handler normal100k simulation accepted — 2026-10-06

The original full normal100k handle finished successfully at executed `4f9303953eeac96fc7dd0d825168e1fa9ea9f1b6`, without restart or timeout changes. All122 seeded workloads complete seeds1–100000 (12,200,000 bodies), all392 pinned regressions and179 top-level tests pass; only the two prescribed trace replay/minimization utility tests skip. Actual SDK elapsed16631.84s is within its original300m timeout. Recorded model counters cover12,402,947 generated schedules,179,962,817 scheduler choices and2,709,420,449 transport events.

The independent terminal reviewer binds1803 Git/before/after/current source inputs, the actual observed SDK binary/profile and closed producer identities. A second full event-stream and complete archive/current-file census check confirms all1840 members. The actual SDK uses normal instrumentation, GOMAXPROCS2/GOMEMLIMIT512MiB and SIM_SEEDS100000. Complete proof is in [the terminal directory](scale/outer-handler-cancellation-2026-10-06/full-normal100k-terminal/).

Together with the already accepted full122×1000 race run and392 pins, this closes the outer-handler simulation normal/race graph gates at their recorded sources. It does not establish NATS disk/Raft behavior, newer unrelated source acceptance, full13×200 Tier2/full16×200 Tier3, million physical timer drain, active-writer GC or actual24h completion. The original24h journal handle remains active; new partition200 run37486948256 remains queued with only its generator job at the latest observation.


## Full Tier2 200-seed singleton row review prepared — 2026-10-06

Added public REST collection and independent full-row review requiring exactly201 terminal successful job identities and400 raw/source provider artifacts, exact singleton seeds1–200, full provider ZIP digest/member readback and complete recorded Git source selection. Every seed runs the existing raw named10m/fault/latency/three-history-model reviewer; stored green summaries cannot replace raw evidence. Duplicate/missing/failed/mixed-source jobs, incomplete provider listings and changed extraction/source inputs reject. Whole collection bytes must remain unchanged.

Six collector/full-row guard groups plus eight shard groups pass. The actual historical partition ZIP validates all five original members with no original changes; sourcefcf2e94 selects1893 inputs/28040118 bytes. Actual queued37486948256 is refused before collection-root creation, with the refusal retained as a tool control rather than a native failure. [Validation and commands](scale/current-tier2-partition-2026-10-06/full-row-review-preparation/).

This prepares the concrete complete200-row gate while the original new campaign remains queued. No new seed/fullrow/fullmatrix/native SDK/store/actual24h acceptance is claimed. Existing hosted and24h handles continue unchanged. Fullnormal100k at4f93039 is now accepted separately; its full archive passed S3 body readback.


## Domain retirement with fresh frame weak absence after all-server SIGKILL — 2026-10-06

New focused race component qualifies at4853571: native body33.81s/actual SDK34.94s. Every original2.15 server is reaped with SIGKILL; three fresh PID/server identities confirm actualWFRETIRE metadata by5.4407s from the whole cut, under original30s. After healing, the exact fresh checkpoint frame returns one forced weak Object Store absence. Production manifest repair confirms native domain leader metadata, then reads actual frame bytes; three selected attempts/one leader confirmation/zero target-body direct oracle requests, final generation3 pointer exact. This is a synthetic weak client response combined with real server/process/payload behavior, not natural follower lag or a server-cause claim.

Strict generation1→3, two collected objects, shared/survivor/fresh reference retention, three effects/two terminals, two fresh handler entries/one manifest loss, all-peer results/raw integrity/quiescent GC pass. Original60s scenario and production lease profile remain. Independent review binds1885 source inputs, actual race SDK/profile, six closed server incarnations and all2322 full archive members, without reopening stores. Six guard groups pass, including actual-log wrong-generation/object/reads/route/count and slow-heal negatives. [Complete proof](scale/domain-retirement-weak-frame-2026-10-06/).

Persistent domain CI includes a third combined row; hosted acceptance is separate. This closes the focused ordinary domain/frame-absence/manifest-repair/server cut. Lease-expiry combined weak absence, legacy versions, active-writer GC, full current-source matrices, original million physical timer drain and actual24h remain open. The original24h journal SDK and new partition200 observer remain active unchanged; no current fullmatrix or soak result is inferred.


## Domain weak frame repair across production lease expiry and all-server SIGKILL — 2026-10-06

Focused race component qualifies at0b2716b: body42.13s/actual SDK43.33s. Every original2.15 domain server is reaped via SIGKILL and held down13.0009s against production12s lease TTL. Three new PID/server identities confirm actualWFRETIRE metadata by19.4935s from whole-cut start (original30s), and terminal epoch72 exceeds prior54. After healing, production manifest repair recovers the exact fresh frame from one controlled weak object absence through one real domain leader confirmation and three selected payload attempts; final generation3 pointer exact, zero target-body direct oracle requests. This is controlled client weak absence with real native process/lease/payload behavior, not natural follower lag or a NATS bug claim.

Original60s scenario/production heartbeat3s/AckWait13s remain. Strict generation1→3, two collected objects, shared/survivor/fresh references, three effects/two terminals, two fresh handler entries/one manifest loss, all-peer results/raw integrity/quiescent GC pass. Independent review binds1886 source inputs, actual race SDK/profile, six closed server incarnations and all2323 complete archive members without reopening stores. Eight guard groups pass, including actual-log short-outage/TTL/non-increasing-epoch/missing weak response/wrong-generation/missing-SIGKILL negatives. [Complete proof](scale/domain-retirement-weak-frame-expiry-2026-10-06/).

Persistent domain CI adds the fourth combined row; hosted acceptance remains independent. This closes the focused domain frame-absence/manifest-repair/lease-expiry/server cut. Legacy versions, other publication timings, active-writer GC, full current-source matrices, original million physical timer drain and actual24h remain open. Original24h producer and partition200 observer remain active unchanged.

## Legacy combined domain retirement control prepared — 2026-10-06

Extend the accepted current-server domain/frame-absence/lease-expiry cut to three
actual2.11.17 peers, preserving the same production lease, strict retirement
assertions and original30s whole-cut/60s scenario/3m SDK deadlines. Select fallback
provisioning and prove versions and backend at admission/restart; retain the
legacy input and exact observed executable bytes for all six incarnations.
Current-version and renamed results cannot stand in for legacy evidence. The
opt-in CI runner preserves complete originals; native acceptance remains
pending. [Preparation](scale/domain-retirement-legacy-weak-frame-expiry-2026-10-06/).

## Legacy combined domain retirement accepted — 2026-10-06

Actual clean97d12e8 race case passes on three2.11.17 peers, including all-server
SIGKILL held beyond production12s TTL, same-domain replacement/fallback checks,
strict retirement/reuse and fresh weak-frame native leader confirmation. Higher
terminal epoch55→73, whole-cut heal19.4985s under original30s,1894 exact inputs,
six closed legacy incarnations and full2352-member originals independently verify.
Initial failure remains unqualified and preserved. Ten guard groups pass; other
legacy combinations/full matrices/million physical drain/actual24h remain open.
[Accepted focused proof](scale/domain-retirement-legacy-weak-frame-expiry-2026-10-06/).

## Committed manifest lost-ack domain retirement control prepared — 2026-10-06

The prior combined manifest-loss/frame-absence/lease-expiry tests inject absence
before publication. Add the complementary real committed-write control: complete
CreateManifest, read back exact fresh manifest bytes/generation/object/positive
stream sequence before SIGKILL, then hold all domain peers beyond productionTTL
and return ErrUnknown after restart instead of delivering the acknowledgement.
Require the same strict reuse/epoch/frame/integrity/GC and original30s/60s/3m
budgets. Prove commit occurs before every kill and final frame matches it;
prepublication or renamed evidence is insufficient. Eleven verifier groups and
compilation pass; native acceptance remains pending.
[Prepared fixture](scale/domain-retirement-committed-manifest-2026-10-06/).

## Committed manifest lost-ack domain retirement accepted — 2026-10-06

Actual clean782d525 race case passes after real generation3 manifest commit and
exact byte readback at positive KV sequence11 before all-server SIGKILL. Outage
exceeds production12s TTL; all domain peers heal18.43795s under original30s.
Committed frame restores with exactly one fresh initial entry/one commit/drop,
strict reuse/shared effects/integrity/GC, native weak-frame confirmation and
higher epoch54→68.1896 exact inputs/six closed incarnations/full2333-member
original archive independently verify. Twelve verifier groups pass. Initial
failed prepublication-count assertion remains preserved; no deadline relaxation.
Other publication/legacy/natural-lag/fullmatrix/million/actual24h gates remain
open. [Focused accepted proof](scale/domain-retirement-committed-manifest-2026-10-06/).
