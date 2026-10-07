# Native recoverable blob port component

At **ed8a92f32a91839c1b234d9fed27496d679eaddf**, actual retained race SDK **2691090** passes the full `internal/blobpublication` package in **32.80s**, count1/two Go CPUs/512MiB/3m. The new object cases have an explicit **30s per-case parent**; existing native authority cases retain their original20s parents. All **2,345** selected committed source inputs match Git, retained copies, and before/after records. The actual live SDK identity/executable hash/clean revision/race profile and terminal process are independently verified.

`NativePort` implements the full experimental protocol port over a new isolated ObjectStore stream and the existing native durable authority. It uploads acknowledged chunks directly, then publishes standard read-compatible ObjectStore metadata with expected subject sequence0. Chunk IDs encode the unique physical upload name, so interrupted uploads are discoverable before metadata exists. Collection writes a permanent attempt tombstone before purging its chunks. Delayed metadata expecting sequence0 is rejected at the server; late chunks remain discoverable orphans. Each operation checks the explicit file-backed, unlimited, non-TTL, non-direct-read format. Ordinary `ObjectStore.GetBytes` remains compatible. No SDK `Put` retry/cleanup operation is used or qualified.

Fourteen native scenario proofs cover:

- R1/R3 shared content, multiple roots, deduplicated references, standard reads, retirement, and final chunk reclamation.
- R1/R3 all three chunks committed with metadata held before publication. Collection tombstones the attempt; forwarding the original metadata packet yields actual server CAS rejection10071/10164. The connection-loss variant cancels the held packet; **this is controlled connection loss, not process SIGKILL**.
- R1/R3 first chunk held before arrival. Collection closes generation1 and commits generation2 of the same content. The old upload later becomes a physical orphan, is revoked before root publication, and is reclaimed without harming the new root.
- R1/R3 metadata committed and bytes independently readable while the actual server reply is buffered. Caller cancellation produces failed preparation without ready-state adoption or destination publication. The ambiguous upload is reclaimed.
- R1/R3 **every peer gracefully shut down, joined, and replaced on the same stores**. Live bytes and permanent attempt tombstones survive; stale metadata is rejected. This does not qualify OS process SIGKILL or power loss.
- Opaque unknown chunks block collection before physical deletion. Thirteen native-created unsafe configurations are rejected by the adapter; NATS itself rejects the fourteenth rollup-with-purge-denied combination. Missing buckets are not created, and post-open retention changes block census/deletion.

Full TCP framing is independently parsed for all paused/late/reply-loss cases: exact held/forwarded bytes, three original chunk publications/content hashes, standard metadata digest, delivered chunk acknowledgments, INFO identities, and native CAS errors where expected. Reply-loss proof binds committed metadata/standard reads, buffered reply bytes, caller cancellation, unpromoted authority, and unpublished root; no unseen reply body is invented. Server INFO/Varz commits identify the embedding application revision, not separately captured upstream NATS source.

The offline reviewer rejects **111 actual-positive proof substitutions**. The complete **3,120-member** fixture archive includes original stores, actual SDK, captured source, native JSON/wire proofs, and packaging records. Original standalone seeded/concurrency/authority tests also pass in the same retained package run. [Independent review](independent-review.json).

The initial uncommitted configuration-fixture failure is retained separately and remains failed. The successful retained SDK was not rerun after the producer's packaging closure check found its authoring shell still referencing the root: that packaging failure is preserved, then a separate completion process establishes closure and captures the full fixture.

## Remaining integration and acceptance

This is an experimental dedicated bucket and native publication destination. Production still uses the existing quiescent collector. Required work remains: migrate all invocation/signal/journal/result/state/snapshot and retained-history roots to atomic durable publication destinations; establish authority lifecycle permissions and migration; qualify OS process kill, arbitrary partitions, lost root replies, and broader concurrent writer/collector faults; prove retention/GC lifecycle at runtime scale. Attempt/generation/head tombstones remain permanent until a separate epoch-compaction protocol is implemented. Current full124 graph, full native matrices, actual24h, original million physical drain, and default dependency adoption remain independent gates.

S3 preservation and local retirement are recorded in the committed readback and removal receipts when complete. Restore the full archive into a fresh directory before further inspection; restoration starts no broker.

Complete S3 archive/metadata/inventory readbacks are verified. Fresh remote member/inventory/closure checks permit retiring the closed original root, archive, and packaging staging: **120,631,296 allocated bytes (115.0 MiB)** reclaimed. [Removal ledger](../reclaimed/README.md).
