# Experimental atomic native snapshot publication

The optional `journal.NativeSnapshotPort` publishes an ordinary version 1 archive or version 2 continuation snapshot together with its retained object graph in one canonical native authority root. The caller's original manifest revision is preserved through `Protocol.PrepareAt`; preparation cannot silently rebase a stale writer. Logical archive/result/signal/frame names resolve through the canonical root's pinned physical references, without a separate alias index.

The graph includes referenced step and terminal results, consumed signal payloads, the current continuation frame and its promise outcomes, and promise outcomes from older checkpoint frames retained in the archival prefix. Dependency bytes and declared hashes are checked before publication. `Store.WriteSnapshot`, `WriteCheckpointSnapshot`, full reads and fast checkpoint reads use the optional transport. Prefix purge remains a separate operation after manifest/object verification. Superseded archive/frame lookups cause a whole-manifest retry; absent current canonical bytes fail closed even if a legacy copy exists.

`ImportSnapshot(ctx, key, legacyStore)` verifies the legacy full journal, archive prefix, any live continuation completion anchor and original manifest revision before publishing the imported graph. Stop legacy snapshot readers/writers and route all subsequent snapshot operations for the workflow through this port before prefix purge. Retain the legacy manifest until migration is reviewed. `RetireManifest` advances the permanent destination head before GC can reclaim the graph.

## Scope and remaining work

This is an explicit experimental adapter. It does not enable production online collection. Staging writes still use the legacy source bucket; the native collector only owns its dedicated recoverable bucket and protected authority. All users of an authority must agree on one native object bucket and preserve authority heads/generation records. Never sweep the legacy bucket with this protocol. Existing production collection remains quiescent.

Invocation, signal, live journal, workflow state, terminal and retained-history destinations still need their own publication fences, migration and lifecycle integration. Permission/privileged stream lifecycle controls, arbitrary partition and committed-read conformance, full scale/message-size qualification, whole runtime integration and final-source release gates remain open. The canonical root currently contains a flattened graph: large graphs must pass real native maximum-payload qualification before adoption; no truncation or relaxed limit is implemented.

## Development probes

The initial failed probe is preserved under `initial-development-probe-failure/`. Its checkpoint assertion counted the anchor as a suffix record, and its upload-race hook did not store the acknowledged bytes. Corrected race probes and exact development sources are retained under `development/`; these logs are development evidence, without retained executable/media admission. They do not qualify the full implementation plan.
