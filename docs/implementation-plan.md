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
