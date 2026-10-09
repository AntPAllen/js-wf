# Worker reader-expiry CLI

The worker accepts `-graph-reader-expiry`, `-graph-reader-expiry-interval` and `-graph-reader-expiry-budget`. Admission requires graph selection, enabled repair loops, an explicit interval in `(0,10s]`, and an explicit budget from 1 through 256. Supplying settings without enabling the feature is rejected before plugin loading or connecting. Default behavior keeps the loop disabled.

The enabled worker starts `RunGraphReaderExpiryWithStore` as an independently leased loop alongside canonical Start/Signal/terminal/timer repair. It uses the selected JetStream instance, including domain options, and participates in normal cancellation/join handling. Terminal audit configuration is independent.

## Verification scope

`TestReaderExpiryCLIAdmission` covers invalid selections, the maximum accepted budget, default disabled selection, and CLI rejection before an unused plugin can load. `TestWorkerRunnerCanonicalGraphReaderExpiry` uses the real runner and matching race-built handler plugin with native R1 and R3 `WFGRAPH` domain clusters. It retains reserved Start/Signal recovery, native timer completion, two missing terminal restorations, unchanged canonical journal, acknowledged repair events and zero legacy journal writes. It additionally inserts a live and an expired reader pin on the completed canonical graph, waits for expiry, verifies live-pin/application/graph preservation, compares native object reference inventories, and checks the persisted version-2 scoped checkpoint. Domain request tracing checks for zero wrong API prefixes.

`current-inputs.json` hashes the six directly relevant sources before the corrected run. It is a focused input record, not a complete repository provenance manifest. Raw output is in `race.log`; `result.json` records the actual child exit zero, package time 123.517 seconds, R1 time 25.52 seconds and R3 domain time 94.31 seconds. Domain tracing observed 25,496 selected-domain API requests and zero wrong prefixes. The recorded sources remained unchanged. GOMAXPROCS=4 matches the VM CPU count; this run overlapped existing race campaigns and does not establish exclusive-CPU performance.

## Development failures retained

`development-build.log` preserves an initial fixture cleanup call with the wrong `ReleaseReader` signature. The optional cleanup call was removed; the native fixture is destroyed by cluster teardown. `development-pin-conflict.log` preserves the first native run: R1 fixture admission hit a definite CAS conflict, while R3 domain passed all expiry and existing repair assertions. The corrected fixture reloads root authority and retries only definite conflicts when inserting test pins, keeping the original expiry and deadline. No runtime retry behavior or deadline was relaxed. The specific concurrent writer responsible for the observed conflict is not established by that log.

## Remaining requirements

This is focused worker wiring verification, not native process/VM kill, route/storage fault, full migration or production adoption. Reader expiry does not collect objects; a completed pass is not a garbage-collection permit. Clock-skew admission and large-catalog throughput remain open. Existing 155 seeded families/835 traces are unchanged; complete current-source normal/race/all-pin/extended qualification remains open. Both live frozen race campaigns and the queued frozen full155 normal campaign exclude this later CLI work. All original broader runtime/fault/scale/soak/drain/migration/release gates remain open.

[Configuration](../../graph-reader-expiry.md), [native scheduler/facade restart controls](../graph-reader-maintenance-facade-2026-10-09/README.md), and [seeded scheduler qualification](../graph-reader-maintenance-simulation-2026-10-09/README.md).
