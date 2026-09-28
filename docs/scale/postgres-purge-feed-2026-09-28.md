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

This isolates sink event throughput. It does not time 10,000 full workflow
purges or replace the plan's running-workflow retention proof. The projector
still rebuilds from retained invocations at startup and every 15 minutes to
repair a crash between tombstone publication and purge-event publication.
