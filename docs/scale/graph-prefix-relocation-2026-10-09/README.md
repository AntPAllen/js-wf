# Graph prefix relocation — 2026-10-09

## Implementation

`PreparePrefixCompaction` copies the live prefix into the reserved `archive` forest and rebuilds the live suffix from index zero. Every relocated payload has a new physical receipt and owner grant, even when its content hash matches an old object. Shared hashes use one new receipt with origin locations in each required forest. Archive indices remain stable across successive compactions. Other named streams, retained snapshots and application bytes are preserved or explicitly supplied by the caller.

`CommitPrefixCompaction` compares the captured source, exact record bytes and payload hashes, preserved forests/readers and new payload/node grants before original-head CAS. Inherited archive frontiers must survive. Lost acknowledgments require exact confirmed readback. This primitive does not interpret journal cursor bytes or enable public continuation admission.

## Executed controls

- 32 deterministic seeds, 64 successive compactions: independent original record/payload bytes, archive/live counts, old-reader reads after collection, every original receipt preserved while pinned and deleted after release, append afterward, complete reclamation after retirement.
- Named start/input/queue forests remain byte-identical and physically readable; 13 records reuse one original owned payload. Compaction copies that receipt and collection deletes the original.
- Ten publication cases: definite drop, confirmed lost acknowledgment, unconfirmed readback, intervening append/reader/retirement, pending expiry, collection during final CAS, revoked grant, changed reader set. Exact retry confirms uncertain successful publication; stale original heads are rejected.
- Eight preparation cases: payload bound/hash/missing bytes, definite or ambiguous upload failure, cancellation and invalid cut. Failed preparation publishes nothing; failed upload orphans are reclaimed.

Final four compaction groups pass normal **0.580s** and race **8.358s**. The 42 existing/new `TestGraph` model groups pass normal **4.069s** and race **56.850s**. Those broader runs compiled before the additional collector-at-CAS test; the final targeted normal/race runs include that case. Production code did not change between those runs. No group skips. The initial development run exposed a missing anticipated head during staged root validation; that bug was fixed before these logs.

Commands: `go test [ -race ] ./internal/graphpublication -run '^TestGraph' -count=1 -v` and the final targeted `-run '^TestGraphPrefixCompaction'` runs. Logs, observed input hashes and executed review accompany this report. This is development evidence, not frozen-source release qualification or the full shared simulation corpus.

## Remaining work

Journal logical offsets and explicit cursor schema migration must map logical archived/live indices correctly. Checkpoint-triggered compaction, retained child ownership, native R1/R3 compaction/collector and fault qualification, history/audit/offline/import/deployment compatibility, bounded compaction resource use, full current/extended simulation and all original broader gates remain open. Archive payloads remain physically retained for audit until lifecycle retirement. Public continuation admission and production collection remain disabled. No existing gate or failure verdict is relaxed.
