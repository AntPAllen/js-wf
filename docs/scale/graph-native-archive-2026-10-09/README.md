# Native journal archive lifecycle — 2026-10-09

## Scope and implementation

The same archive lifecycle scenario now runs on seeded in-memory storage and native R1/R3 JetStream domains. The scenario is extracted into a shared helper; no production code or default configuration changes in this commit. Native configuration provisioning is explicit, replica counts are enforced, and all API/object/authority operations use the `ARCHIVE` domain.

The native test stops every server after the first compaction, confirms all are stopped, and restarts the original file stores/ports. After metadata quorum readiness, it opens fresh authority/object adapters and the public native journal store with v6 selected. The existing old view retains its handle, but its storage adapter is rebound to the reopened native authority; reads still require canonical persisted reader validation. No new pin is acquired for that old view and no workflow cursor/root/object store is recreated.

This is a graceful full-server store restart, not an OS process kill or power loss. Dispatch/source metadata uses the modeled Signal transport. The journal bodies, grants, application cursors, reader pins, copies and collection are real native storage operations; this does not qualify autonomous native worker execution.

## Assertions

For both R1 and R3:

- Two checkpoint compactions, unchanged absolute logical records/sequences and complete terminal history audit.
- Adapter reopen and full server restart between the first compaction and further reads/collection.
- Old view remains readable after restart and collection; its original physical receipt becomes unreadable after release and a later collection.
- Fresh view reads both the archive and live suffix with new receipts; checkpoint lookup and a live logical owned append succeed.
- Archived grant append, wrong tail, terminal compaction and v5 cursor adoption are rejected.
- Retirement and collection leave zero enumerated physical objects; an independent raw object-stream subject census has no chunk (`.C.`) subjects and no truncated subject accounting.

Native normal passes **13.091s** (R1 **1.89s**, R3 **11.19s**). Refactored 16-seed model and configuration groups pass normal **1.954s**. Combined native/model/configuration race passes **68.389s**, with native R1 **17.60s**, native R3 **30.83s** and the 16 model cases. All three top-level groups pass with no skips. Original normal adapter-only reopen evidence is retained separately.

Two initial full-restart runs failed at R3 authority admission with a request context timeout; no server-side cause is established. The fixture now waits for the restarted metadata quorum within the original two-minute context. Original logs remain preserved. No latency acceptance threshold, recovery target, fixture deadline or production retry behavior changed.

Commands: `go test ./journal -run '^TestNativeGraphCheckpointArchiveReopenAndCollection$' -count=1 -v`; `go test ./journal -run '^TestGraphCheckpointArchive' -count=1 -v`; and combined `go test -race ./journal -run '^(TestNativeGraphCheckpointArchiveReopenAndCollection|TestGraphCheckpointArchive.*)$' -count=1 -v`. Input hashes, successful and failed logs, and executed review accompany this report. This is development component evidence, not independent frozen-source/full shared simulation qualification.

## Remaining work

Native publication/collection/Start/Signal/retirement contention and ambiguous compaction replies; malformed v6 cursor controls; worker-triggered compaction and continued SDK execution with state, promise results and pending-child provenance after collection; real process/storage/power faults; resource/scale/capacity qualification; offline/history/import/old deployment migration and public admission; all original full/extended/native matrices/24h/million physical-drain/dependency/release gates. Production online collection remains disabled. Full 148-family/748-pin frozen qualification excludes this addition.
