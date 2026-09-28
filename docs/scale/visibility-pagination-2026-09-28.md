# PostgreSQL visibility page check — 2026-09-28

The optional three-node/PostgreSQL integration test covers two-page status and
attribute queries, durable catch-up, rebuild, purge, and ID reuse. It passes
with PostgreSQL 18 in a local container.

For a separate query-plan check, I inserted 100,000 synthetic `scale` rows into
the local `wf_visibility` table, split evenly between `completed` and `queued`.
One in ten had `team=alpha`. After `ANALYZE`, `EXPLAIN (ANALYZE, BUFFERS)` for
a 101-row page after `('scale','id090000')` showed:

| Query | Plan | Rows read or filtered | Execution |
| --- | --- | ---: | ---: |
| `status=completed` | `wf_visibility_status_idx` index scan | 101 returned | 0.111 ms |
| `status=completed AND team=alpha` | same ordered index scan, attribute filter | 101 returned, 404 filtered | 0.361 ms |

Both plans applied the seek predicate and stopped after the page filled. These
are local synthetic query measurements, not end-to-end workflow throughput or
a proof under concurrent updates. The attribute GIN index is present, but the
planner chose the ordered status index for this selective late-page query.
