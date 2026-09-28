# PostgreSQL purge-feed scale check

The opt-in `TestPostgresPurgeFeedScale` measures the PostgreSQL visibility
consumer against a real three-node JetStream cluster and PostgreSQL 16. It
materializes 10,000 rows in `wf_visibility`, then publishes 10,000 distinct
events to the durable `WF_PURGE` work queue. Every tenth row has a newer
invocation sequence than its event, testing the generation check at scale.

```sh
WF_PG_PURGE_SCALE=1 \
WF_TEST_POSTGRES_DSN='postgres://.../disposable_db' \
go test ./integration -run '^TestPostgresPurgeFeedScale$' -count=1 -timeout=10m -v
```

| Run | Events | Load and drain time | Rows remaining | Result |
| --- | ---: | ---: | ---: | --- |
| Original serial loop | 1,000 | 33.3 s | 100 | Pass |
| Short journal wait during purge backlog | 1,000 | 2.3 s | 100 | Pass |
| Short journal wait during purge backlog | 10,000 | 16.3 s | 1,000 | Pass |
| Repeat with exact surviving-ID and sequence audit | 10,000 | 15.4 s | 1,000 | Pass |
| Race-instrumented diagnostic | 1,000 | 2.6 s | 100 | Pass |

The test waits for a sentinel purge event to be acknowledged before loading
rows, so the projector's startup rebuild cannot clear the fixture. It checks
that `Lag()` reaches zero, the PostgreSQL row count is exact, and the work
queue has no retained messages. The existing PostgreSQL tests separately
exercise real `retention.Purge`, a reused invocation ID, and writer-lock loss.

The event-only test isolates sink event throughput. A second opt-in mode now
adds PostgreSQL to the full `TestTenThousandConcurrentPurgesAndThousandReusedIDs`
workflow proof:

```sh
WF_PURGE_REUSE_SCALE=1 WF_PURGE_REUSE_ACTIVE=1 \
WF_PURGE_REUSE_POSTGRES=1 \
WF_TEST_POSTGRES_DSN='postgres://.../disposable_db' \
go test ./integration \
  -run '^TestTenThousandConcurrentPurgesAndThousandReusedIDs$' \
  -count=1 -timeout=30m -v
```

It waits for PostgreSQL to index all 10,000 completed original workflows
before purging. While 10,000 other handlers are executing, 64 callers purge
the original IDs and start 1,000 new generations during the purge. After
all workflows finish, it waits for journal and purge lag to reach zero,
verifies all 11,000 final SQL rows and invocation sequences, checks that the
purge work queue is empty, and runs the retained-stream integrity audit.

| End-to-end run | Old rows indexed | Final state | Result |
| --- | ---: | --- | --- |
| Original 16-entry journal batches | 5m21s | SQL rows reached the expected shape; lag exceeded the 20-minute context | Failed timing gate |
| 256-entry journal batches and backlog rebuild | 42.7 s | 11,000 exact SQL rows, zero lag, empty purge queue, integrity audit | Passed in 4m29.7s |
| Retained-message scan and durable cursor reset | 29.5 s | 11,000 exact SQL rows, zero lag, empty purge queue, integrity audit; three rebuilds and three cursor resets | Passed in 3m02.6s |

The projector now uses a larger journal fetch for PostgreSQL. When at least
5,000 journal entries await processing, it can rebuild from retained
invocations with 32 bounded workers. Its ordered `WF_INV` reader queues only
retained messages, avoiding purged sequence holes; a 1,000-hole fixture passed.
On NATS 2.15+, a successful rebuild can advance the PostgreSQL journal durable
to the sequence after its pre-rebuild watermark. Older servers retain the
per-message acknowledgment fallback. The full active-handler run confirmed
three backlog rebuilds and three successful cursor resets. A 1,000/1,000/100
active race run passed in 1m29s with one of each.

The projector still rebuilds at startup and every 15 minutes to repair a
crash between tombstone and event publication. The full proof has passed on
one three-node fixture on the expanded VM; leader faults during the combined
PostgreSQL and retention load remain unverified.
