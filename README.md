# JetStream durable workflow runtime

An early Go implementation of the attached durable workflow plan. It uses NATS JetStream for write-once invocations, CAS journals, dispatch, leases, terminal results, and large input objects. The integration suite boots three real `nats-server` nodes in process.

[Implementation status](docs/implementation-status.md) tracks each phase against the plan.

## What works

- `provision.EnsureAuto` selects native scheduling on NATS 2.12+ or retained fallback timers on older servers, then creates and checks the stores. An existing `WF_RUN` stream fixes the mode on later calls. `provision.Ensure` and `provision.EnsureFallback` remain available for explicit configuration.
- `client.Start` accepts one input per `(type, id)`, retries uncertain publishes with bounded attempts, returns `ErrAlreadyStarted` for identical retries and `ErrInputMismatch` for changed input. Inputs over 900 KiB use `WF_BLOB`.
- `journal.Append` uses per-subject expected sequence CAS and rereads the subject tail before classifying a wrong-sequence reply as stale; it retries an unchanged-tail rejection twice. `journal.Read` checks index continuity, epoch order, and terminal ordering; it retries a bounded whole read when compaction advances a snapshot during the live scan. Workers snapshot after 256 new entries, retain 16 live entries, and purge the covered journal prefix and consumed signals.
- Workers reserve space for a terminal `Failed` entry when an invocation approaches the 100,000-entry journal limit.
- `lease.Acquire` uses KV creation revisions as fencing epochs. Renew and release use revision CAS.
- `worker.RunPartition` consumes a durable partition, renews the lease and message progress, replays journaled steps, and saves one terminal result in `WF_STATE`. `Worker.Metrics()` reports lease acquisitions, fencing, redeliveries, and enqueue-to-lease latency for that worker.
- `worker.WithPartitionConcurrency(n)` allows up to `n` run messages in flight per partition loop (1–32; default 1). Invocation leases still serialize the same workflow. The 10,000-sleep three-node proof needed 32 per partition to meet the plan's under-two-second p99 handler completion target on this VM; see [`docs/scale/timer-sleep-2026-09-28.md`](docs/scale/timer-sleep-2026-09-28.md).
- `worker.RunAssigned(ctx, index, count)` gives each of a fixed set of workers a disjoint share of all 64 partitions; `RunPartitions` accepts an explicit set. `assignment.Store.InitializeStatic` fills missing owners in `WF_ASSIGN`, `Assign` moves a partition with revision CAS, and `worker.RunKVAssignments` watches ownership changes. A three-node test moves a partition while a step is running and completes it on the new owner. A separate three-node opt-in test completes 10,000 short invocations with six static workers and a test KV marker that detects simultaneous execution of one invocation. Set `WF_REBALANCE_SCALE=1` to run the 200-invocation, 50-step rebalance test; it moves busy partitions every five seconds and audits all results and journals. Set `WF_REBALANCE_KV_KILL=1` to run the same load while killing the assignment KV leader after the first moves; workers stay on the surviving quorum. Set `WF_REBALANCE_WORKER_PARTITION=1` to isolate an active worker connection for 45 seconds during the moving backlog. Set `WF_REBALANCE_WORKER_KILL=1` to close an active worker connection and start a replacement with the same ID while moves continue. Set `WF_REBALANCE_PROCESS_KILL=1` to run one owner in a child process, send SIGKILL after it enters a handler, and verify that the other workers complete the moving backlog. Set `WF_PROCESS_PAUSE=1` to stop a worker process for 45 seconds past its lease expiry and verify its stale journal append is rejected after resuming. Set `WF_REBALANCE_ROUTE_PARTITION=1` to isolate one NATS server from the other two for 45 seconds while six workers keep processing and four busy partition assignments keep moving; the test audits 200 results and their journals after the routes heal. Set `WF_REBALANCE_COMBINED_CHAOS=1` to overlap that route cut with an in-flight worker process kill, a 45-second process pause, and a 45-second worker connection cut during the same 200-workflow rebalance; the resumed process must receive `journal.ErrStale` on its old tail. A separate three-node test moves ownership while the old worker is network isolated, checks the single terminal result after lease expiry, and verifies that 40 follow-up invocations go only to the new owner after reconnect. Workers cancel partition loops on disconnect, refresh the KV watch before resuming, and reconcile KV revisions every second to recover missed updates during a KV leader change. Set `WF_REPEATED_CONSUMER_KILL=1` to run 1,000 queued invocations while killing and restarting the current `WF_RUN` consumer leader three times; the worker retries bounded JetStream operations and the test audits every terminal journal.
- `wf assignment-init owner-a owner-b` fills missing owners; `wf assignment-get 0` reports the current owner and revision; `wf assignment-move 0 owner-b REVISION` moves one partition if its revision still matches. Run workers with matching IDs through `RunKVAssignments` to follow these changes.
- Handler panics outside recorded steps are journaled as numbered attempts. Workers retry them with exponential backoff capped at five minutes and durably fail the invocation after three attempts by default; `worker.WithMaxPanicAttempts` changes the limit. A committed final attempt is completed as `Failed` on recovery without running the handler again.
- `wf.Run` journals a request before an effect and its serialized outcome afterward. It detects changed step names and declared inputs on replay. Results above 900 KiB use `WF_BLOB` and a verified journal reference. Effects can run more than once if a worker stops between effect execution and completion append.
- `wf.RunOnce` supplies a stable key scoped to the invocation sequence for downstream deduplication. `wf.Now` journals a server timestamp and `wf.Random` journals a random value, so both replay consistently.
- `cmd/wf-lint` reports direct `time.Now` and random-package calls inside functions that take `*wf.Context`. It accepts Go files or directories and exits nonzero on findings. It skips `wf.Run` and `wf.RunOnce` effect callbacks, whose results are journaled. The source scan does not follow calls into separate helpers.
- `wf.Replay` runs a function against serialized journal records without NATS; callers supply referenced Object Store bytes through `ReplayOptions`.
- `wf.Context.SetState` and `GetState` journal writes and observed reads. Replay rebuilds state in call order, including after journal compaction.
- `wf.Sleep` schedules a server-side wakeup and suspends without retaining a goroutine. The fire time comes from a JetStream server timestamp; completion uses the wakeup message's server timestamp.
- `wf.Context.Timer` returns a durable handle with `Await`, `SelectSignal`, and `Cancel`. A workflow can create several timers before awaiting; due awaits can complete in one run. `SelectSignal` journals whether a named signal or timer won, with a buffered signal taking priority when both are ready. Cancellation is journaled, so a later scheduled wakeup cannot turn it into an observed fire.
- Scheduled wakeups carry their invocation sequence and timer step. A delayed wakeup from a purged invocation is acked without running a newer invocation that reused its ID. `Worker.Metrics()` also reports scheduled and completed timers, wakeup delivery lateness buckets, and cancelled timer wakeups acked as no-ops.
- On fallback deployments, workers retain timer records in `WF_TIMER` and `reconcile.RunFallbackTimerLoop` polls them using server time. The poller deletes a due record only after `WF_RUN` acknowledges its wakeup, and removes records whose generation has a matching purge marker or tombstone. Run this loop alongside the regular timer reconciler. This path passed against NATS 2.11.17 using `WF_NATS_SERVER_BIN` in the opt-in integration test. [NATS per-message TTL](https://docs.nats.io/learn/jetstream/message-ttl) deletes unread messages on expiry, so TTL cannot be the dispatch trigger in a durable fallback.
- `client.Signal` and `wf.AwaitSignal` buffer, journal, and replay external signals. Large signal payloads use `WF_BLOB`; callers supply stable idempotency keys. `client.SignalWithStart` explicitly creates a missing invocation with its input before buffering the signal, and rejects a concurrent start with different input. `client.SignalWithOptions` can require an invocation to be running and returns `ErrNotRunning` when the journal is already terminal. Completion can race the check because signal and journal writes use separate streams. Large terminal results also use `WF_BLOB`; `client.Await` verifies the object hash.
- `client.Cancel` publishes an idempotent reserved control signal. On its next dispatch, the worker journals the signal and a terminal `Failed` outcome; `client.Await` returns `ErrCancelled`. A workflow that completes before the worker handles cancellation keeps its original outcome.
- `wf.Call` and `wf.CallAsync` start children with deterministic IDs scoped to the parent invocation sequence. Child results are signalled only to that generation; a late child cannot signal a reused parent ID. Three-node tests cover a parent interrupted during 500-child fan-out and a three-deep chain whose middle result signal is lost before publication and repaired on worker restart.
- `reconcile.RunStartLoop`, `RunSignalLoop`, `RunTimerLoop`, and `RunSuspendedLoop` repair missing starts, signal wakeups, and overdue timers. The suspended scan starts from journaled waits and reports dry-run candidates. Their scan cursors persist with KV revision CAS across leader changes. `integrity.Check` examines retained streams and terminal KV values after quiescence.
- `retention.Purge` retires a completed invocation in signal → journal → tombstone → invocation order, removing retained fallback timers for that generation between journal and tombstone. `retention.Handler` runs it as a durable workflow. Clients receive `ErrPurged` while a tombstone is live; a reused ID has a new invocation sequence that fences old signals and wakeup deduplication keys.
- `retention.SweepTombstones` removes expired tombstones with KV revision checks after the retired invocation has disappeared. Run `wf-cli sweep-tombstones` periodically to reclaim them.
- `retention.SweepBlobsQuiescent` reclaims unreferenced `WF_BLOB` objects after all clients and workers stop. It marks references from retained invocation and signal headers, live journals, terminal state, and snapshot objects. It cannot run safely alongside blob writers because Object Store deletion has no revision precondition.
- `visibility.Projection` consumes journal changes into `WF_VIEW` rows and status indexes. `Rebuild` reconstructs rows from retained invocations and logical journals, including snapshot prefixes, and removes purged rows. `Lag` reports pending journal messages for its durable consumer.
- `cmd/wf` provides `project`, `list`, `describe`, `lag`, `export-journal`, `cancel`, `purge`, `sweep-tombstones`, `scan-suspended`, and `journal-capacity` commands. `cancel` reports that its request was accepted; the worker records the outcome when it runs. The suspended scan defaults to dry-run; `-apply` publishes its candidates. Use `-rebuild list` for an immediate source-of-truth refresh when the projection loop is not running. `journal-capacity` prints current `WF_JRN` byte utilization and exits nonzero at 70% so an external monitor can poll it; it requires a configured journal byte cap.
- `testcluster` boots a three-node cluster, can kill a node, and records reproducible fault schedules from a seed. Its client TCP proxy can isolate and heal a pinned worker or SDK connection; tests cover an idle worker reconnect and an in-flight worker partitioned for 45 seconds, with a surviving worker completing the invocation and a stale append rejected by journal CAS. A scheduled-timer test fully restarts all three servers twice before the fire time. A 10,000-publish test kills a stream leader during concurrent writes, checks every retained sequence and acknowledged payload on a survivor, and verifies that a one-replica control fails the same audit.
- `client.NewObserved` records `Start`, `StartChild`, `Signal`, and `Await` call intervals and hashed arguments. `history.Recorder` exports JSON lines. `history.CheckStarts` checks write-once starts, including uncertain publishes. `history.CheckSignals` checks stream order and idempotent retries, with a Porcupine model for each key; uncertain publishes use a branching model. `history.CheckResults` checks stable terminal values, purge, and ID reuse by invocation generation. Three-node tests check 500 concurrent starts, 10,000 signals from 100 callers, and concurrent result readers against recorded histories.
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
./wf-cli -url nats://localhost:4222 describe math job-1
./wf-cli -url nats://localhost:4222 export-journal math job-1 > journal.json
./wf-cli -url nats://localhost:4222 cancel math job-1
./wf-cli -url nats://localhost:4222 journal-capacity
```

## Design corrections and limits

The plan proposes one atomic batch across `WF_INV` and `WF_RUN`. [JetStream atomic batches are scoped to one stream](https://docs.nats.io/learn/jetstream/advanced-publishing#atomic-batch-publish), so this implementation uses two writes and a leased reconciler. It publishes directly to a deterministic partition subject instead of relying on a stream subject transform. The lease creation revision is the epoch; that remains unique if a worker dies before its first journal append.

JetStream requires a schedule and target in the same stream, and the server rejects schedules with `DiscardNew` ([NATS scheduler discussion](https://github.com/nats-io/nats-server/discussions/7363)). `WF_RUN` therefore has scheduling enabled with `DiscardOld` and no message, byte, or age limit. Provisioning rejects later limits that could evict live work.

This is **not a production complete runtime**. Timer backend selection checks the connected server version when `WF_RUN` is absent; it does not certify every peer during a mixed-version rollout. Aggregated metric export, full scheduled-message recovery, retention cleanup at scale, operator `replay`, automated partition rebalance and reassignment stress tests, and most fault injection are outstanding. The query projection scans retained invocations during rebuild and all KV keys during listing; large-deployment SQL sinks, custom search attributes, and scale tests beyond 100,000 distinct starts remain. Snapshots rewrite the full logical prefix and need scale testing. Tombstone sweeping scans the KV bucket and must be scheduled by the deployment; blob reclamation currently requires a stopped runtime and has no online writer coordination. Scheduled messages and child invocations created before generation headers were added are not protected against ID reuse; drain or retire them during an upgrade. Previously journaled RunOnce keys and child IDs use the older derivation, so finish those invocations before switching to this version. The test fixture can isolate client connections and inter-server routes; in-process server pauses and slow disks remain. No full chaos or linearizability claim is made.

The worker supports sequential durable operations. Workflow handlers should not issue steps concurrently. An effect that is not idempotent can execute again after a crash; use downstream idempotency keys when needed. A canceled effect that ignores its context may continue running after the worker stops waiting for it, while a later worker retries from the journaled request. Signal idempotency relies on JetStream's configured duplicate window, so a retry after that window can store another signal. The signal history checker refuses histories longer than that window. Histories recorded in one process use that process's clock; cross-VM histories need a shared timing strategy.

## Verify

```sh
go test ./...
go test -race ./wf ./integration
WF_JOURNAL_BOUNDARY=1 go test ./integration -run '^TestJournalMaxEntriesBoundary$' -timeout=25m -v
WF_DISPATCH_SCALE=1 go test ./integration -run '^TestTenThousandShortInvocationsAcrossAllPartitions$' -timeout=15m -v
WF_TIMER_CANCEL_SCALE=1 go test ./integration -run '^TestThousandTimerCancellationsBeforeFire$' -timeout=10m -v
WF_TIMER_SLEEP_SCALE=1 go test ./integration -run '^TestTenThousandRandomSleeps$' -timeout=12m -v
go run ./cmd/wf-lint ./integration
```

The opt-in boundary test fills one three-replica journal with 100,000 entries,
checks the next append is rejected, and reads the retained journal through
another node. The manually triggered `long-journal` CI job also runs it.

For the separate-process subject-cardinality measurement, see
[`docs/scale/README.md`](docs/scale/README.md). The recorded three-replica run
reached 10M subjects in each of `WF_INV` and `WF_JRN` after the VM expansion.
The same document records hot-journal and concurrent CAS append
throughput from `cmd/wf-cas-bench`.
