# PostgreSQL visibility namespace isolation — 2026-10-09

`PostgresStore.Namespace` selects a separate table, status/attribute indexes,
rebuild cleanup and advisory writer lock. Empty retains the legacy table and
lock. Configure a distinct, immutable namespace for each source account/store.
This isolates projections; it does not bind a namespace to its source or provide
PostgreSQL permissions isolation.

Identifiers contain a fixed prefix and 128 bits of SHA-256, so namespace text is
never interpolated into SQL. The longest index identifier is 63 bytes. Namespaced
writer keys are negative, preserving separation from the positive legacy lock;
64-bit advisory-key collisions can conservatively serialize distinct namespaces.

## Executed component checks

Local PostgreSQL 16.15, Docker image
`sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea`,
owned disposable container `js-wf-pg-namespace-20261009`, loopback port 33518.
Both commands explicitly set `WF_TEST_POSTGRES_DSN`; logs must contain PASS,
not skipped tests.

```sh
go test -race ./visibility -run '^TestPostgresNamespaceIsolation$' -count=1 -v
go test -race ./integration -run '^TestPostgres(VisibilityProjection|IncrementalPurgeAndLateEvent|RebuildSkipsPurgedInvocationHoles|VisibilityWriterSessionLoss)$' -count=1 -v
```

The namespace control exercises identical IDs in three stores, SQL punctuation
in a namespace, upsert/status/attribute/seek queries, generation-aware deletion,
rebuild cleanup without affecting other namespaces, same-namespace contention,
independent legacy/other writers and lock release. Native legacy controls cover
projection/rebuild, late purge events, source holes and session loss.

These tests ran in the development checkout, not a frozen qualification source.
Graph projection and CLI still reject PostgreSQL mode; integrating the isolated
sink and qualifying canonical rebuild/lag/retirement remains required. No scale,
full simulation, soak or release gate is established by these component checks.
