# Graph reader expiry

The experimental graph worker can remove expired reader pins in bounded batches. Enable the loop with an explicit cadence and budget:

```sh
wf-worker \
  -url "$NATS_URL" \
  -id graph-worker-1 \
  -handler-plugin ./handlers.so \
  -graph-authority-stream EXAMPLE_GRAPH_AUTH \
  -graph-authority-prefix wf.graph.example \
  -graph-object-bucket EXAMPLE_GRAPH_OBJECTS \
  -timer-backend native \
  -graph-reader-expiry \
  -graph-reader-expiry-interval 5s \
  -graph-reader-expiry-budget 16
```

Use an already provisioned isolated graph namespace selected consistently by clients and workers. These values illustrate configuration; capacity at large workflow counts remains unqualified. The interval must be positive and at most ten seconds. The budget must be from 1 through 256 roots per batch. These flags require graph selection and enabled `-reconcile` loops. Invalid combinations are rejected before plugin loading or connection. The loop is disabled by default.

Reader expiry uses the GraphStore maintenance facade, an independent system lease, and an atomic version-2 checkpoint in `WF_STATE`. The lease/checkpoint names include a hash of the authority stream, subject prefix and object bucket. Each pass captures a catalog watermark, persists it before inspecting roots, and resumes bounded batches across worker replacement. A completed pass resets the cursor; later passes include newly cataloged roots. Cadence applies between scheduler iterations, including checkpoint creation and individual batches, so a catalog pass can take several intervals.

Expired pins are pruned using canonical root CAS. Live pins, application bytes and object storage remain intact. A completed catalog pass is not permission to delete objects. Production collection stays off. The expiry clock is the native scheduler's wall clock; deployments must keep reader and maintenance clocks consistent. This feature does not establish clock-skew admission, atomic lifecycle/purge ordering, process/VM kill qualification or retention collection.

[Facade and native restart evidence](scale/graph-reader-maintenance-facade-2026-10-09/README.md) and [seeded scheduler evidence](scale/graph-reader-maintenance-simulation-2026-10-09/README.md) describe the underlying verified scope.

[Worker CLI verification](scale/graph-reader-expiry-cli-2026-10-09/README.md) includes R1 and R3 domain integration and configuration admission.
