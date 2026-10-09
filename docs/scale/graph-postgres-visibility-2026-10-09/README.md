# Canonical graph PostgreSQL visibility — 2026-10-09

Graph projection now uses an explicitly namespaced PostgreSQL sink. Configure
the same logical namespace in `WithGraphJournal(graph, namespace)` and
`WithPostgres(&PostgresStore{DB: db, Namespace: namespace})`. Missing or mismatched
namespaces are rejected. Use a distinct namespace for each source account/store;
this is configuration isolation, not a source-binding or authorization mechanism.

CLI `project`, `list` and `lag` accept the three canonical graph flags plus
`-postgres-dsn` and `-graph-view-namespace`. PostgreSQL mode rejects
`-graph-view-bucket`; KV mode requires a separately provisioned bucket and rejects
`-graph-view-namespace`. Validation occurs before connecting. The namespace must
not be one of the reserved runtime bucket names.

Each bounded catalog refresh writes a fresh generation and only deletes other
generations after every source read succeeds. Errors preserve prior rows rather
than certifying absence. Canonical root/source validation, pinned history,
projector-local verified cache, lease scheduling hints, retirement, attributes,
seek pagination and lag use the same graph paths as KV. SQL writer locks cover
Run/Rebuild and are independent of legacy projections. Graph projection creates
no legacy journal or purge-feed consumer.

The implementation polls the bounded catalog and upserts all discovered rows
per cycle. Large-scale/event-consumer performance and adoption remain open.
PostgreSQL is a query projection, never workflow authority.

## Component evidence

The owned disposable PostgreSQL 16.15 container binds only to loopback.
Commands explicitly set `WF_TEST_POSTGRES_DSN` so PostgreSQL checks do not skip:

```sh
go test -race ./visibility ./cmd/wf -run '^(TestGraph(Postgres)?VisibilityCanonicalRowsAndUncertainty|TestGraphVisibilityConfiguration|TestGraphOperatorRejectsPartialOrLegacyOnlySelection|TestNativeCanonicalGraph(Postgres)?OperatorCommands)$' -count=1 -v
```

`component-race.log` is the first PostgreSQL native/model run. Controls were
subsequently strengthened: unrelated stale rows must survive failed scans and be
removed after successful scans; missing/mismatched namespaces and invalid CLI
sink combinations are rejected; native post-shutdown rebuild checks that the
purged row is absent. `final-controls-race.log` covers the strengthened model
and admission controls; `final-race.log` repeats them with native KV/PostgreSQL
R1/R3-domain fixtures and the post-shutdown purge assertion.

Native tests run the real CLI projector subprocess at 100 ms while a worker
handles large owned input and Signal bodies. They query PostgreSQL without
requesting a competing rebuild while the projector holds its writer lock, then
perform a rebuild after shutdown. Existing Start/Signal/result, forged mirror,
online/offline replay, scans, terminal repair, cancellation and purge assertions
remain. Both backends verify one effect, zero legacy journal API requests,
correct domain API prefix and joined SIGTERM shutdown.

Model source scenarios are deterministic; the SQL backend itself is a real
PostgreSQL server. This does not establish deterministic PostgreSQL fault
simulation. Six scenarios cover ordinary/cached query repair, catalog uncertainty,
forged native source, held/unknown leases and retirement, including attributes,
lag and no input-body reads.

These are development component checks, not frozen complete-suite qualification.
The full race campaign at `9a1ccdc` excludes these changes. All original remaining
full/extended simulation, runtime/continuation/snapshot/import/deployment/GC,
native partition/kill, scale, actual24h, physical-drain, adoption and release gates
remain required.
