# Public native graph SDK — focused verification accepted

Frozen code **7e272f1** exposes `journal.NativeGraphConfig`, `OpenNativeGraphStore`, `NativeGraphStreamConfigs`, `GraphPayloadLink` and `GraphOwnedPayload`. Applications can configure the opt-in worker/client and reuse verified payload edges without importing internal packages. Stream/bucket names and optional expected replica counts are checked before opening. Native admission preserves existing graph structural/configuration checks; the constructor never creates, updates, imports or collects.

| Check | Normal | Race |
| --- | ---: | ---: |
| Eleven focused graph journal groups | 10.432s | 60.695s |
| Native graph worker R1/R3 through public constructor | 4.051s | 20.865s |

The new native journal SDK group covers R1/R3, missing stores without implicit creation, read-only admission, expected-replica mismatch, protobuf journal entries, public owned-edge reuse, fresh reopened store reads, retained old snapshot payloads and generation retirement rejection. A separate native admission group rejects a MaxAge authority and legacy ObjectStore, preserving original configuration/sequences/legacy bytes. Configuration controls reject invalid namespaces, limits, encoding and replica selections. These are constructor/component controls; the underlying full unsafe-config matrices remain covered by the graph adapter's own qualification, not newly rerun here.

The existing native worker test now constructs its store through the public SDK API, exercising large input/signal/result replay and closed-fixture drain. Its three replacement workers are gracefully stopped objects, not crashed processes or restarted peers.

A separate **graph-sdk-consumer** module compiles the public configuration, worker/client constructors and payload-reuse call. Its source, go.mod, go.sum, command and output are retained. The package has no tests, so Go's JSON terminal action is `skip`; exit0 establishes compilation only. Native runtime execution is established by the separate native tests above. The module uses an explicit local replace to the frozen repository checkout; this is not a published-module/version or deployment qualification.

All **1,400** selected tracked inputs matched the frozen Git revision before execution, remained unchanged afterward and were checked again during review. Four native normal/race commands and one external compilation command ran once under their original five-minute timeout, two-Go-CPU/512MiB envelope. Raw logs, before/after hashes, runner/reviewer and consumer hashes are retained. No skipped native tests or data-race reports are accepted.

Existing simulation families and503 pins were not modified. The previous10,000 normal/1,000 race worker-family evidence remains at its earlier frozen0d63f3e scope; it is not reclassified as full current-source qualification. Canonical start/input/signal/state/terminal/snapshot/continuation/import/reconciler/retention/history/deployment migration remains open, as do native reader capacity/concurrency/partitions/scale and actual process/storage crash qualification, full133-workload current-source simulation, full native matrices, original24h, million physical drain, dependency/default adoption and release gates. Production collection remains quiescent.
