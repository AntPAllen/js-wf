# Terminal projection audits

The experimental graph worker can periodically verify present terminal projections by dispatching canonical terminal recovery. Enable auditing explicitly with a separate cadence and scan budget:

```sh
wf-worker \
  -url "$NATS_URL" \
  -id graph-worker-1 \
  -handler-plugin ./handlers.so \
  -graph-authority-stream EXAMPLE_GRAPH_AUTH \
  -graph-authority-prefix wf.graph.example \
  -graph-object-bucket EXAMPLE_GRAPH_OBJECTS \
  -timer-backend native \
  -graph-terminal-audit \
  -graph-terminal-audit-interval 5s \
  -graph-terminal-audit-budget 16
```

Use an already provisioned isolated graph namespace selected consistently by clients and workers. The cadence and budget above illustrate configuration, not a qualified capacity recommendation. Interval must be positive and at most ten seconds; budget must be positive. Audit flags require graph selection and enabled `-reconcile` loops. Supplying audit settings without enabling the audit is rejected before plugin loading or connection.

The existing missing-projection reconciler keeps `-reconcile-interval` and `-reconcile-budget`. Auditing adds an independent `graph-terminal-audit` cursor and system lease; it does not replace missing-projection discovery or change the default worker configuration. Each audit pass inspects at most its configured catalog budget. Healthy projections also generate worker verification work because catalog metadata does not contain their canonical payload digest.

Workers use owned canonical terminal bytes to repair mismatches with revision CAS. A current/newer valid purge marker suppresses auditing. Malformed JSON/markers and uncertain lookups stop that scan instead of authorizing a write. Canonical journal/effects remain authoritative; a projection is a cache. Audit mode does not enable production collection, import legacy state, recover malformed projections, or establish atomic cross-store lifecycle/purge ordering.

[Implementation and seeded/native evidence](scale/graph-terminal-audit-2026-10-09/README.md) and [worker CLI verification](scale/graph-terminal-audit-cli-2026-10-09/README.md) describe the tested scope. Audit throughput at large workflow counts and full current-source release qualification remain open.
