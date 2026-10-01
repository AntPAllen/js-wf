# JetStream durable workflow runtime

An early Go implementation of the attached durable workflow plan. It uses NATS JetStream for write-once invocations, CAS journals, dispatch, leases, terminal results, and large input objects. The integration suite boots three real `nats-server` nodes in process.

The [repository implementation plan](docs/implementation-plan.md) includes a detailed Tier 1 deterministic simulation design. The [journal, lease, start, and dispatch simulation slices](docs/tier1-simulation.md) each run 1,000 seeds in the default tests; [implementation status](docs/implementation-status.md) tracks what is actually verified.

## What works

- `provision.EnsureAuto` provisions retained fallback timers on a fresh deployment and accepts an existing fallback stream. It rejects an existing native stream because one connected server cannot prove cluster-wide scheduling support. Use `provision.Ensure` for explicit native mode only after checking every peer; `provision.EnsureFallback` remains available for explicit fallback mode.
- `client.Start` accepts one input per `(type, id)`, retries uncertain publishes with bounded attempts, returns `ErrAlreadyStarted` for identical retries and `ErrInputMismatch` for changed input. Inputs over 900 KiB use `WF_BLOB`.
- `journal.Append` uses per-subject expected sequence CAS and rereads the subject tail before classifying a wrong-sequence reply as stale; it retries an unchanged-tail rejection up to 40 times. `journal.Read` checks index continuity, epoch order, and terminal ordering; after 64 live entries it uses one filtered pull consumer to avoid one server lookup per entry. The long-read retry path also runs in seeded Tier 1 simulation. It retries a bounded whole read when compaction advances a snapshot during the live scan. Workers snapshot after 256 new entries, retain 16 live entries, and purge the covered journal prefix and consumed signals.
- Workers reserve space for a terminal `Failed` entry when an invocation approaches the 100,000-entry journal limit.
- `lease.Acquire` uses KV creation revisions as fencing epochs. Renew and release use revision CAS.
- `worker.RunPartition` consumes a durable partition, renews the lease and message progress, replays journaled steps, and saves one terminal result in `WF_STATE`. `Worker.Metrics()` reports lease acquisitions, fencing, redeliveries, and enqueue-to-lease latency for that worker. `worker.MetricsHandler(workers...)` exports aggregate counters and cumulative latency histograms in Prometheus text format. The `wf-worker` process serves these at `/metrics`; embedded applications can mount the handler on their own HTTP server.
- `worker.WithPartitionConcurrency(n)` allows up to `n` run messages in flight per partition loop (1–32; default 1). Invocation leases still serialize the same workflow. The 10,000-sleep three-node proof needed 32 per partition to meet the plan's under-two-second p99 handler completion target on this VM; see [`docs/scale/timer-sleep-2026-09-28.md`](docs/scale/timer-sleep-2026-09-28.md).
- Set `WF_FULL_RESTART_TIMERS=1` to run the Linux process-cluster proof that suspends 1,000 native timers, restarts all three server processes before any is due, then verifies every retained schedule, due-time boundary, result, journal, and drained run queue. The `timer-1000-restart` workflow runs it manually in CI.
- Set `WF_TIMER_FANIN_SCALE=1` to run the three-node 3,000-timer fan-in proof. Each workflow journals server time to derive a replay-stable duration to one shared UTC second; the test checks the actual `fire_at` spread, every result and journal, per-partition `MaxAckPending`, and burst delivery rate. The `timer-fanin-3000` workflow runs it manually in CI.
- Set `WF_TIMER_FANIN_10000=1` to run the same fan-in proof at 10,000 timers. The `timer-fanin-10000` workflow runs the larger burst manually in CI.
- `worker.WithDispatchTiming(ackWait, heartbeat)` configures durable redelivery and progress cadence (default 13s/3s; AckWait is the 12s lease TTL plus 1s). Workers sharing a partition must use the same `AckWait`. The route-fault timer test uses 10s/2s and measures its 30-second recovery p99 from the later of timer due time and final confirmed route heal; three full 10,000-timer runs passed that definition.
- When upgrading an existing deployment from a different AckWait, stop workers sharing the durable partitions and explicitly update each existing `WF_P_XX` consumer's `AckWait` to 13s using JetStream `UpdateConsumer` with its retained configuration. Preserve the durable and its pending deliveries. New workers fail closed on a configuration mismatch; deployments can retain 20s temporarily with `WithDispatchTiming(20*time.Second, 3*time.Second)` until the coordinated update.
- `worker.RunAssigned(ctx, index, count)` gives each of a fixed set of workers a disjoint share of all 64 partitions; `RunPartitions` accepts an explicit set. `assignment.Store.InitializeStatic` fills missing owners in `WF_ASSIGN`, `Assign` moves a partition with revision CAS, and `worker.RunKVAssignments` watches ownership changes. A three-node test moves a partition while a step is running and completes it on the new owner. A separate three-node opt-in test completes 10,000 short invocations with six static workers and a test KV marker that detects simultaneous execution of one invocation. Set `WF_REBALANCE_SCALE=1` to run the 200-invocation, 50-step rebalance test; it moves busy partitions every five seconds and audits all results and journals. Set `WF_REBALANCE_KV_KILL=1` to run the same load while killing the assignment KV leader after the first moves; workers stay on the surviving quorum. Set `WF_REBALANCE_WORKER_PARTITION=1` to isolate an active worker connection for 45 seconds during the moving backlog. Set `WF_REBALANCE_WORKER_KILL=1` to close an active worker connection and start a replacement with the same ID while moves continue. Set `WF_REBALANCE_PROCESS_KILL=1` to run one owner in a child process, send SIGKILL after it enters a handler, and verify that the other workers complete the moving backlog. Set `WF_PROCESS_PAUSE=1` to stop a worker process for 45 seconds past its lease expiry and verify its stale journal append is rejected after resuming. Set `WF_REBALANCE_ROUTE_PARTITION=1` to isolate one NATS server from the other two for 45 seconds while six workers keep processing and four busy partition assignments keep moving; the test audits 200 results and their journals after the routes heal. Set `WF_REBALANCE_COMBINED_CHAOS=1` to overlap that route cut with an in-flight worker process kill, a 45-second process pause, and a 45-second worker connection cut during the same 200-workflow rebalance; the resumed process must receive `journal.ErrStale` on its old tail. A separate three-node test moves ownership while the old worker is network isolated, checks the single terminal result after lease expiry, and verifies that 40 follow-up invocations go only to the new owner after reconnect. Workers cancel partition loops on disconnect, refresh the KV watch before resuming, and reconcile KV revisions every second to recover missed updates during a KV leader change. Set `WF_REPEATED_CONSUMER_KILL=1` to run 1,000 queued invocations while killing and restarting the current `WF_RUN` consumer leader three times; the worker retries bounded JetStream operations and the test audits every terminal journal.
- `assignment.Plan(liveWorkers, currentOwners)` produces a deterministic balanced ownership plan. `assignment.Rebalance(ctx, store, liveWorkers, dryRun)` reads all retained revisions and applies only required moves through CAS. Existing owners keep partitions up to their quota; conflicts are left for the next pass and uncertain writes are resolved by rereading. Callers can supply a live membership snapshot or use the automatic membership controller.
- A regular three-node automatic-membership test SIGKILLs an actual worker/coordinator inside a durable effect, checks expiry-driven reassignment to the surviving worker, and audits 65 ten-step workflows and their Start/Await histories. Its focused race runs recovered all results in 15.1–15.6 seconds. Seeded simulation separately exercises the same controller pass through virtual KV expiry and dropped/hidden assignment writes; broader automatic-membership chaos remains open.
- `wf assignment-init owner-a owner-b` fills missing owners; `wf assignment-get 0` reports the current owner and revision; `wf assignment-move 0 owner-b REVISION` moves one partition if its revision still matches. Run workers with matching IDs through `RunKVAssignments` to follow these changes.
- `wf-worker -mode auto` registers its unique ID in the optional `WF_MEMBERS` bucket before consuming work. Registrations expire after 12 seconds of server time and renew every three seconds. One revision-fenced coordinator balances all 64 partitions over live IDs; workers follow `WF_ASSIGN`. Duplicate live IDs are rejected, and membership or controller failures stop the runner. Graceful shutdown releases registration; hard crashes recover through expiry. All automatic workers must share the same handler set. Automatic mode owns the assignment map, so use one assignment policy across a deployment. Bucket expiry and replication settings are verified at startup.
- Build `go build -o wf-worker ./cmd/wf-worker`, then run `./wf-worker -id owner-a -handler-plugin ./handlers.so -mode kv` after initializing assignments. A Go plugin can export `var Handlers map[string]worker.Handler` or `func Handlers() map[string]worker.Handler`, or a map/factory of `worker.WorkflowDefinition` containing initial handlers and named continuations, and must use the same Go toolchain and source version as the worker binary. The process provisions stores, retries transient connection and leader-recovery errors for up to 30 seconds at startup, runs its assigned partitions, serves metrics at `127.0.0.1:9090`, and starts leader-elected start, signal, timer, suspended, and tombstone repair loops, plus the fallback timer poller when `WF_RUN` uses fallback scheduling. Use `-mode static -static-index I -static-count N` to divide partitions among fixed workers; the default static settings own all 64 partitions. Set `-replicas` to the desired stream replica count (1–5, default 3). The default `-timer-backend auto` provisions fallback scheduling even when the connected node is new, and rejects an existing native stream. `-timer-backend native` requires an operator-verified fully upgraded cluster; `-timer-backend fallback` explicitly selects the retained poller mode. Existing `WF_RUN` mode cannot be silently changed. `-retention-type retention` also registers the built-in durable purge handler with `-retention-grace` (default 24h); start it with JSON input such as `{"type":"orders","id":"order-42"}` and a unique purge-workflow ID.
- Add `-events-file /path/owner-a.jsonl` to retain actual fencing and repair publication decisions. Use a separate file per worker process. Records include worker ID, PID, process start time, per-session sequence and the typed event (including acknowledgement or uncertainty). Existing complete files are appended; an incomplete final line is refused. The asynchronous queue holds 256 records; overflow or file errors stop the worker with an error. Graceful shutdown drains and syncs the file. SIGKILL can lose queued or unsynced records, so this diagnostic log alone cannot certify complete hard-kill attribution. See [process event proof](docs/scale/worker-process-events-2026-10-01/).
- Set `-journal-max-bytes` on the worker runner to provision an exact positive `WF_JRN` byte cap. Every `/metrics` scrape reports current journal bytes and configured limit; capped streams also report `js_wf_journal_capacity_ratio`. The [Prometheus alert rule](docs/monitoring/prometheus-rules.yml) fires at 70% for two minutes. Configure a `cluster` scrape label and route its `severity: warning` alert through your Alertmanager receiver. A failed journal lookup makes the scrape return HTTP 503.
- The manual `worker-metrics-soak` workflow runs a real worker and JetStream server for 15 or 60 minutes, completing workflows and scraping `/metrics` after each result. It requires every scrape to succeed, lease-acquisition counters to stay monotonic and cover completed workflows, journal-byte samples to remain available, and the process to shut down cleanly.
- Set `WF_TIER3_CONTAINER=1` to run the opt-in [five-container smoke](docs/scale/tier3-container-smoke-2026-09-29.md). Its Docker route network can isolate one NATS server while host-pinned clients stay connected; the test checks majority starts, signal idempotency, and immutable results with Porcupine, heals routes, restarts that server on its original file store, and audits one completed workflow. The manual `tier3-container-smoke` workflow also runs five-replica leader-kill, leader-pause, and quorum-loss publish audits in CI, and uploads the client history. Set `WF_TIER3_SYNC_INTERVAL` to a positive duration or `always` to match the server setting used in production; the fixture default is the pinned server’s `2m` interval.
- Handler panics outside recorded steps are journaled as numbered attempts. Workers retry them with exponential backoff capped at five minutes and durably fail the invocation after three attempts by default; `worker.WithMaxPanicAttempts` changes the limit. A committed final attempt is completed as `Failed` on recovery without running the handler again.
- `wf.Run` journals a request before an effect and its serialized outcome afterward. It detects changed step names and declared inputs on replay. Results above 900 KiB use `WF_BLOB` and a verified journal reference. An unserializable result becomes a typed, journaled step error, so a retained completion replays without rerunning the effect. Effects can run more than once if a worker stops between effect execution and completion append.
- `wf.RunOnce` supplies a stable key scoped to the invocation sequence for downstream deduplication. `wf.Now` journals a server timestamp and `wf.Random` journals a random value, so both replay consistently.
- `cmd/wf-lint` reports direct `time.Now` and random-package calls inside functions that take `*wf.Context`. It accepts Go files or directories and exits nonzero on findings. It skips `wf.Run` and `wf.RunOnce` effect callbacks, whose results are journaled. The source scan does not follow calls into separate helpers.
- `wf.Replay` runs a function against serialized journal records without NATS; callers supply referenced Object Store bytes through `ReplayOptions`.
- `wf.Context.SetState` and `GetState` journal writes and observed reads. Replay rebuilds state in call order, including after journal compaction.
- `wf.Sleep` schedules a server-side wakeup and suspends without retaining a goroutine. The fire time comes from a JetStream server timestamp; completion uses the wakeup message's server timestamp.
- A regular three-node test retains a real 30-day `wf.Sleep` schedule across two full cluster restarts. The opt-in `WF_TIER3_LONG_TIMER=1` five-container test additionally advances every server's verified Go wall clock by 31 days on the second restart, observes the retained native timer fire, checks its unchanged journal prefix, and reads the same terminal result through all five nodes. The `tier3-container-smoke` workflow includes this advanced-clock proof in a separate job.
- Set `WF_TIMER_RECONCILE_SCALE=1` to run the three-node proof that 200 journaled timer requests with no scheduled publishes stall without a reconciler and all complete after `RunTimerLoop` starts. Set `WF_TIMER_PROCESS_KILL_SCALE=1` to run the stronger proof that SIGKILLs 200 real worker processes after each timer request is journaled but before its scheduled publish, then verifies that all 200 recover through the reconciler. See [`docs/scale/timer-schedule-kills-2026-09-28.md`](docs/scale/timer-schedule-kills-2026-09-28.md).
- `wf.Context.Timer` returns a durable handle with `Await`, `SelectSignal`, and `Cancel`. A workflow can create several timers before awaiting; due awaits can complete in one run. `SelectSignal` journals whether a named signal or timer won, with a buffered signal taking priority when both are ready. Cancellation is journaled, so a later scheduled wakeup cannot turn it into an observed fire.
- `wf.Select(c, wf.SignalAwaitable("ready"), promise, timer)` waits on any combination of SDK signals, child promises and timers. The first ready argument wins; the recorded choice survives replay even if other cases become ready later. It returns the selected index and signal/child bytes (nil for timers). Propagate `ErrSuspended` when none are ready. Losing awaitables remain available; cancel unwanted timers explicitly.
- Scheduled wakeups carry their invocation sequence and timer step. A delayed wakeup from a purged invocation is acked without running a newer invocation that reused its ID. `Worker.Metrics()` also reports scheduled and completed timers, wakeup delivery lateness buckets, and cancelled timer wakeups acked as no-ops.
- On fallback deployments, workers retain timer records in `WF_TIMER` and `reconcile.RunFallbackTimerLoop` polls them using server time. The poller deletes a due record only after `WF_RUN` acknowledges its wakeup, and removes records whose generation has a matching purge marker or tombstone. Run this loop alongside the regular timer reconciler. This path passed against NATS 2.11.17 using `WF_NATS_SERVER_BIN` in the opt-in integration test. [NATS per-message TTL](https://docs.nats.io/learn/jetstream/message-ttl) deletes unread messages on expiry, so TTL cannot be the dispatch trigger in a durable fallback.
- `client.Signal` and `wf.AwaitSignal` buffer, journal, and replay external signals. Large signal payloads use `WF_BLOB`; callers supply stable idempotency keys. `client.SignalWithStart` explicitly creates a missing invocation with its input before buffering the signal, and rejects a concurrent start with different input. `client.SignalWithOptions` can require an invocation to be running and returns `ErrNotRunning` when the journal is already terminal. Completion can race the check because signal and journal writes use separate streams. Large terminal results also use `WF_BLOB`; `client.Await` verifies the object hash and retries bounded reads across transient leader failures.
- `client.Cancel` publishes an idempotent reserved control signal. A running worker observes it through a shared NATS subscription, cancels the handler context, then journals the consumed signal and terminal `Failed` outcome after the handler returns; `client.Await` returns `ErrCancelled`. The worker also checks the durable signal stream at handler registration and every 15 seconds to cover missed notifications. A workflow that completes before cancellation is observed keeps its original outcome. Call `Worker.Close` after stopping its partition loops to release the subscription.
- `wf.Call` and `wf.CallAsync` start children with deterministic IDs scoped to the parent invocation sequence. Child results are signalled only to that generation; a late child cannot signal a reused parent ID. Three-node tests cover a parent interrupted during 500-child fan-out and a three-deep chain whose middle result signal is lost before publication and repaired on worker restart.
- `reconcile.RunStartLoop`, `RunSignalLoop`, `RunTimerLoop`, and `RunSuspendedLoop` repair missing starts, signal wakeups, and overdue timers. The suspended scan starts from journaled waits and reports dry-run candidates. Their scan cursors persist with KV revision CAS across leader changes; bounded attempts retry transient stream errors and cursor conflicts. `integrity.Check` examines retained streams and terminal KV values after quiescence.
- `retention.Purge` retires a completed invocation in signal → journal → tombstone → invocation order, removing retained fallback timers for that generation between journal and tombstone. `retention.Handler` runs it as a durable workflow. Clients receive `ErrPurged` while a tombstone remains stored, even if its nominal expiry has passed; the sweeper removes it without relying on the client clock. A reused ID has a new invocation sequence that fences old signals and wakeup deduplication keys. A [10,000/10,000/1,000 concurrent proof](docs/scale/purge-reuse-2026-09-28.md) checks old-signal rejection and every reused journal, including a run with 10,000 other handlers executing during purge.
- `retention.SweepTombstones` removes expired tombstones with KV revision checks after the retired invocation has disappeared. `retention.TombstoneScan` pages through the KV stream with a bounded cursor, and `reconcile.RunTombstoneLoop` persists that cursor under a leader lease. Use `wf-cli tombstone-loop` for scheduled cleanup, `scan-tombstones` for a bounded dry run, or `sweep-tombstones` for a one-shot full scan. A [100,000-tombstone three-node proof](docs/scale/tombstone-sweep-2026-09-28.md) checks the scheduled path.
- `retention.SweepBlobsQuiescent` reclaims unreferenced `WF_BLOB` objects after all clients and workers stop. It marks references from retained invocation and signal headers, live journals, terminal state, and snapshot objects. It cannot run safely alongside blob writers because Object Store deletion has no revision precondition.
- `wf.Context.SetSearchAttributes` journals a replacement map of up to 16 searchable string attributes; replay checks the declared map. `visibility.Projection` consumes journal changes into `WF_VIEW` rows, status indexes, and value-hashed attribute indexes. `ListByAttribute` filters by an attribute and optional status. `WithAttributeMapper` sets a new projection schema version and transforms journaled attributes during rebuild after a field rename. `Rebuild` reconstructs rows from retained invocations and logical journals, including snapshot prefixes, and removes purged rows and indexes. `Lag` reports pending journal messages for its durable consumer. A [50,000-invocation process-kill proof](docs/scale/projection-recovery-2026-09-28.md) drains lag and verifies byte-identical rebuild state.
- `cmd/wf` provides `project`, `list`, `describe`, `lag`, `export-journal`, `export-replay`, `replay`, `cancel`, `purge`, `sweep-tombstones`, `scan-suspended`, and `journal-capacity` commands. `cancel` reports that its request was accepted; the worker records the outcome when it runs. The suspended scan defaults to dry-run; `-apply` publishes its candidates. Use `-rebuild list` for an immediate source-of-truth refresh when the projection loop is not running. `journal-capacity` prints current `WF_JRN` byte utilization and exits nonzero at 70% so an external monitor can poll it; it requires a configured journal byte cap. `replay` loads a user-supplied Go handler plugin, verifies completed results, handler failures, or recorded waits against the journal, and can run from an exported bundle without NATS; see [`docs/replay.md`](docs/replay.md).
- `testcluster` boots a three-node cluster, can kill a node, and records reproducible fault schedules from a seed. Its client TCP proxy can isolate and heal a pinned worker or SDK connection; tests cover an idle worker reconnect and an in-flight worker partitioned for 45 seconds, with a surviving worker completing the invocation and a stale append rejected by journal CAS. A scheduled-timer test fully restarts all three servers twice before the fire time. A 10,000-publish test kills a stream leader during concurrent writes, checks every retained sequence and acknowledged payload on a survivor, and verifies that a one-replica control fails the same audit.
- `client.NewObserved` records `Start`, `StartChild`, `Signal`, and `Await` call intervals and hashed arguments. `history.Recorder` exports JSON lines. `history.CheckStarts` checks write-once starts, including uncertain publishes. `history.CheckSignals` checks stream order and idempotent retries, with a Porcupine model for each key; uncertain publishes use a branching model. `history.CheckResults` checks stable terminal values, purge, and ID reuse by invocation generation. Three-node tests check 500 concurrent starts, 10,000 signals from 100 callers, and concurrent result readers against recorded histories. Run `WF_SIGNAL_FAULT_SCALE=1 go test ./integration -run TestSignalsUnderRoutePartition -count=1 -timeout=10m` for the opt-in 10,000-signal route-fault proof, including ordered journal consumption and a retained-state audit.
- Tests can hide a successful JetStream publish acknowledgment at the SDK boundary or on the TCP connection. They check start enqueue repair, 1,000 committed or absent journal append recoveries, and signal deduplication against the real three-node stores. A start that reads its own committed publish can return `ErrAlreadyStarted`; the history model accepts that response when no separate successful start is observed.

## Quick example

After connecting with `nats.Connect`, creating `js` with `jetstream.New`, and calling `backend, err := provision.EnsureAuto(ctx, js, replicas)`:

```go
handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
    var n int
    if err := json.Unmarshal(input, &n); err != nil { return nil, err }
    value, err := wf.Run(c, "double", n, func(ctx context.Context) (int, error) {
        return n * 2, nil
    })
    if err != nil { return nil, err }
    return json.Marshal(value)
}

w, err := worker.New(ctx, js, "worker-1", map[string]worker.Handler{"math": handler})
if err != nil { return err }
go w.RunPartition(ctx, identity.Partition("math", "job-1", provision.Partitions))
if backend == provision.FallbackTimers {
    go reconcile.RunFallbackTimerLoop(ctx, js, "timer-poller-1", time.Second, 1000)
}

c := client.New(js)
_, err = c.Start(ctx, "math", "job-1", []byte(`21`))
if err != nil { return err }
result, err := c.Await(ctx, "math", "job-1") // JSON bytes: 42
```

For a bounded journal, call `provision.EnsureAutoWithJournalLimit(ctx, js, replicas, maxBytes)` instead. Provisioning verifies the exact cap on later starts and keeps `DiscardNew`, so a full journal stream rejects appends without evicting live entries. Poll `wf journal-capacity` for the 70% warning.

Run the four regular reconciler loops in separate goroutines, plus the fallback timer loop when `EnsureAuto` returns `FallbackTimers`. A deployment must arrange workers for all 64 partitions and register every workflow type each worker may receive.

Run `visibility.Projection.Run` in a separate goroutine or run the `project` CLI command to maintain the query view. For example, after building the CLI with `go build -o wf-cli ./cmd/wf`:

```sh
./wf-cli -url nats://localhost:4222 project
./wf-cli -url nats://localhost:4222 -rebuild list completed
./wf-cli -url nats://localhost:4222 -attribute team=payments list suspended
./wf-cli -url nats://localhost:4222 -limit 100 list completed
./wf-cli -url nats://localhost:4222 describe math job-1
./wf-cli -url nats://localhost:4222 export-journal math job-1 > journal.json
./wf-cli -url nats://localhost:4222 export-replay math job-1 > replay.json
./wf-cli -handler-plugin ./workflow.so -replay-bundle replay.json replay
./wf-cli -url nats://localhost:4222 cancel math job-1
./wf-cli -url nats://localhost:4222 journal-capacity
./wf-cli -url nats://localhost:4222 -budget 256 -interval 100ms tombstone-loop
```

For a larger query view, set `WF_POSTGRES_DSN` (or pass `-postgres-dsn`) on the `project`, `list`, and `lag` CLI commands. The CLI uses PostgreSQL instead of `WF_VIEW` for rows and queries; it creates `wf_visibility` with a status B-tree and a GIN index for exact search-attribute matches. The PostgreSQL projection has separate `WF_VIEW_PG` journal and `WF_VIEW_PG_PURGE` event consumers, so a KV projector can run independently. Run `provision.Ensure` when upgrading to create the retained `WF_PURGE` stream. A PostgreSQL advisory lock rejects concurrent projector writers and `-rebuild` commands while a projector runs. The database pool needs at least two connections. For example:

```sh
export WF_POSTGRES_DSN='postgres://user:password@localhost:5432/workflows?sslmode=require'
./wf-cli -url nats://localhost:4222 project
./wf-cli -url nats://localhost:4222 -attribute team=payments list completed
```

`visibility.WithPostgres(&visibility.PostgresStore{DB: db})` exposes the same `Get`, `List`, `ListByAttribute`, `ListPage`, `ListByAttributePage`, `Rebuild`, `Lag`, and `Run` calls to Go programs. Pass `-limit 1..1000` to the CLI `list` command for a bounded `{rows,next}` response, then pass its `next` value as `-after` with the same filters. PostgreSQL uses indexed keyset queries; KV pages still scan the small-deployment view. Pages reflect the current projection, so rows that change status or attributes between requests can enter or leave later pages. A completed rebuild removes purged rows; an interrupted rebuild leaves the prior rows for the next attempt. Purge publishes a generation-scoped event to the 30-day work queue after the tombstone, and the PostgreSQL consumer deletes only that generation. A 15-minute full rebuild repairs a crash before event publication; on NATS 2.15+, successful rebuilds can advance the PostgreSQL journal durable past the covered watermark; the KV projector retains its 30-second rebuild. The optional PostgreSQL integration test runs with `WF_TEST_POSTGRES_DSN` set to a **disposable database**. An [opt-in 10,000-event PostgreSQL purge-feed check](docs/scale/postgres-purge-feed-2026-09-28.md) drained in 15.4 seconds while preserving 1,000 newer-generation rows; a second opt-in mode completed the full 10,000-purge/10,000-active/1,000-reuse proof with the live PostgreSQL projector, zero lag, and 11,000 exact final SQL rows in 3m02.6s.

## Design corrections and limits

The plan proposes one atomic batch across `WF_INV` and `WF_RUN`. [JetStream atomic batches are scoped to one stream](https://docs.nats.io/learn/jetstream/advanced-publishing#atomic-batch-publish), so this implementation uses two writes and a leased reconciler. It publishes directly to a deterministic partition subject instead of relying on a stream subject transform. The lease creation revision is the epoch; that remains unique if a worker dies before its first journal append.

JetStream requires a schedule and target in the same stream, and the server rejects schedules with `DiscardNew` ([NATS scheduler discussion](https://github.com/nats-io/nats-server/discussions/7363)). `WF_RUN` therefore has scheduling enabled with `DiscardOld` and no message, byte, or age limit. Provisioning rejects later limits that could evict live work.

This is **not a production complete runtime**. Timer backend selection checks the connected server version when `WF_RUN` is absent; it does not certify every peer during a mixed-version rollout. Embedded applications still need to mount the aggregate metrics handler; the `wf-worker` runner serves it directly. Full scheduled-message recovery, retention cleanup at scale, CLI replay of older journal-capacity failures without attempted-step metadata, automatic membership/rebalance fault and soak tests, and most fault injection are outstanding. The KV query projection scans retained invocations during rebuild and all KV keys during listing, including attribute queries. The PostgreSQL sink indexes queries, supports keyset pagination, and consumes generation-scoped purge events; startup and periodic rebuilds still scan retained invocations, and combined PostgreSQL fault and soak tests remain. Scale tests beyond 100,000 distinct starts remain. Snapshots rewrite the full logical prefix and need scale testing. The one-shot tombstone command still scans all KV keys; the scheduled loop pages by stream sequence. Blob reclamation currently requires a stopped runtime and has no online writer coordination. Scheduled messages and child invocations created before generation headers were added are not protected against ID reuse; drain or retire them during an upgrade. Previously journaled RunOnce keys and child IDs use the older derivation, so finish those invocations before switching to this version. The test fixture can isolate client connections and inter-server routes; in-process server pauses and slow disks remain. No full chaos or linearizability claim is made.

The worker supports sequential durable operations. Workflow handlers should not issue steps concurrently. An effect that is not idempotent can execute again after a crash; use downstream idempotency keys when needed. A canceled effect that ignores its context may continue running after the worker stops waiting for it. A handler that ignores its context can delay the terminal cancellation until it returns. Offline replay stops at an incomplete step request without executing the missing effect. Signal idempotency relies on JetStream's configured duplicate window, so a retry after that window can store another signal. The signal history checker refuses histories longer than that window. Histories recorded in one process use that process's clock; cross-VM histories need a shared timing strategy.

## Verify

```sh
go test ./... -timeout=25m
go test -race ./wf ./integration -timeout=25m
WF_JOURNAL_BOUNDARY=1 go test ./integration -run '^TestJournalMaxEntriesBoundary$' -timeout=25m -v
WF_DISPATCH_SCALE=1 go test ./integration -run '^TestTenThousandShortInvocationsAcrossAllPartitions$' -timeout=15m -v
WF_TIMER_CANCEL_SCALE=1 go test ./integration -run '^TestThousandTimerCancellationsBeforeFire$' -timeout=10m -v
WF_TIMER_SLEEP_SCALE=1 go test ./integration -run '^TestTenThousandRandomSleeps$' -timeout=12m -v
WF_SUSPENDED_SCAN_SCALE=1 WF_SUSPENDED_SCAN_FULL=1 go test ./integration -run '^TestMillionSuspendedScanCursorSurvivesLeaderKill$' -timeout=15m -v
WF_TOMBSTONE_SWEEP_SCALE=1 go test ./integration -run '^TestHundredThousandTombstonesSweptByLeaderLoop$' -timeout=15m -v
go run ./cmd/wf-lint ./integration
```

The opt-in boundary test fills one three-replica journal with 100,000 entries,
checks the next append is rejected, and reads the retained journal through
another node. The manually triggered `long-journal` CI job also runs it.

The opt-in timer route-fault gate measures its 30-second p99 from the final
confirmed route heal and passed three full 10,000-timer runs. A more
synchronized 1,000-timer diagnostic still has one recorded p99 miss. The
observed results are in
[`docs/scale/timer-route-faults-2026-09-28.md`](docs/scale/timer-route-faults-2026-09-28.md).

For the separate-process subject-cardinality measurement, see
[`docs/scale/README.md`](docs/scale/README.md). The recorded three-replica run
reached 10M subjects in each of `WF_INV` and `WF_JRN` after the VM expansion.
The same document records hot-journal and concurrent CAS append
throughput from `cmd/wf-cas-bench`.


For the sustained mixed worker-skew row, run
`WF_MATRIX_CHAOS=1 go test -race ./integration -run '^TestMixedMatrixWorkerClockSkew$' -count=1 -timeout=20m`.
It uses +5-second, −5-second and normal-clock worker processes and verifies each
against real JetStream timestamps throughout the default ten-minute workload.
The `tier2-matrix-leaders` workflow exposes this row as `workerclock`; a
`WF_MATRIX_DURATION=35s` run is smoke evidence only.


For sustained single-server ±60-second clock skew, run
`WF_MATRIX_CHAOS=1 go test -race ./integration -run '^TestMixedMatrixServerClockSkew(Positive|Negative)$' -count=1 -timeout=40m`.
Each direction runs a ten-minute mixed workload and verifies clocks and stream
leader placement. The CI selectors are `serverclockplus` and `serverclockminus`;
35-second runs remain smoke evidence only.


For sustained full-cluster restarts at verified unfinished fan-out cuts, run
`WF_MATRIX_CHAOS=1 go test -race ./integration -run '^TestMixedMatrixFanoutRestartEveryThirtySeconds$' -count=1 -timeout=18m`.
The CI selector is `fanoutrestart`. Fault artifacts retain the suspended parent,
six child IDs, unfinished children and journal cut sequence for each restart.


For real five-second filesystem block stalls, run
`WF_MATRIX_CHAOS=1 go test -race ./integration -run '^TestMixedMatrixBlockDiskStallEveryThirtySeconds$' -count=1 -timeout=18m`.
The `blockdisk` CI row checks its privileged fixture first. The fixture requires
Linux device mapper, loop devices, ext4 tools and passwordless sudo. It creates
and removes a private file-backed device for one server's store. Fault artifacts
record suspended/resumed times and the blocked-sync proof; this is separate
from dm-delay per-request injection.


For the sustained rolling-upgrade row, provide a NATS 2.11.17 executable through
`WF_NATS_SERVER_BIN` and run
`WF_MATRIX_CHAOS=1 go test -race ./integration -run '^TestMixedMatrixRollingServerUpgrade$' -count=1 -timeout=18m`.
The `upgrade` CI selector builds that version, starts all three nodes on it,
and upgrades each once during the ten-minute mixed workload. Timers remain on
the verified fallback backend; version transitions are retained in fault artifacts.


To investigate the mixed-version WorkQueue retention regression, run
`WF_NATS_SERVER_BIN=/path/to/nats-2.11.17 MATRIX_ARTIFACT_PREFIX=/tmp/mixed-ack go test -race ./integration -run '^TestMixedVersionExplicitAckWorkQueueRetention$' -count=1 -timeout=8m -v`.
This opt-in transport contract checks out-of-order acknowledgments after a
consumer-leader upgrade. The old-only and upgraded co-located leader controls
pass; an old stream leader with an upgraded consumer leader currently retains
acknowledged records. Split-version cases enforce the retention requirement
and fail until the discrepancy is resolved; they are not clean release evidence.
The `mixed-move-after-ack` recovery control first requires 33 records to remain
stored for thirty seconds, then moves the stream leader onto the upgraded
consumer leader's node and requires the stream to drain. That control passed
locally; it establishes this fixture's recovery action, not a general upgrade
repair protocol. Failure artifacts include actual messages, consumer state and
server logs; the recovery control also saves its pre-move raw queue.

`TestMixedVersionMultipleConsumerRetentionRecovery` adds two durable consumers.
It checks that only the upgraded consumer leader's records remain retained under
an old stream leader, verifies removal after the stream-leader move, then
upgrades the second consumer leader and requires both groups to drain with
separate upgraded leaders. All 132 records are verified with raw sequence
reads. Run it with the same old binary and artifact variables:
`go test -race ./integration -run '^TestMixedVersionMultipleConsumerRetentionRecovery$' -count=1 -timeout=3m -v`.
The `mixed-version-retention-controls` workflow runs these expected bad-state
and recovery controls; a pass does not clear strict mixed-version conformance.
