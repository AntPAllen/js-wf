# Journal archive cursor — 2026-10-09

## Implemented

`ArchiveCheckpoints` explicitly selects new isolated v6 stores and requires the canonical Start/Signal/checkpoint configuration. Existing v5 readers reject v6 even before compaction. Default configurations remain unchanged; native configuration supports explicit selection without provisioning or enabling collection automatically.

The cursor carries `retained_from` as an absolute logical offset. Live population must equal logical count minus that offset; archive population must match the offset, except after retirement when all owned forests are empty. A published checkpoint request cannot precede the live boundary. Reads preserve logical entry indices and sequence numbers across both forests. Appends convert logical source indices to live physical positions and reject archived grants.

`CompactCheckpoint` opens a fresh pin, validates the owned frame and exact tail, releases the pin, then observes the original source head. It requires an active generation and the matching published pointer. The archive cut is the checkpoint request index; request, completion and suffix remain live. Relocation and the matching application cursor publish atomically using the preceding protocol primitive. Matching retries are idempotent; generation/tail/head changes fail closed. Retired lifecycle metadata and sequence high-water marks survive reclamation.

## Evidence

Sixteen seeded transport fixtures vary prefix length from one to sixteen step pairs. Each performs two checkpoint compactions, compares every logical record/sequence with the original pinned history, reads the old view after collection, closes it and confirms an original physical entry receipt is then absent. Fresh full-history reads still work; fresh receipts differ from the originals. A live logical reference is reused in an append, an archived reference is rejected, the newer checkpoint compacts the previous suffix, full terminal audit reads remain valid and retirement/collection leaves zero physical objects. Old v5 readers, wrong-tail and terminal compaction requests are rejected. Invalid library/native archive configuration is rejected.

Final archive groups pass normal **1.448s** and race **22.898s**. All **30 journal `TestGraph` model groups** pass normal **8.249s** and race **112.347s**. The broader runs compiled before adding the final wrong-tail/terminal assertions; production code was unchanged, and the final targeted runs include those assertions. No skips. Client, worker and reconcile packages compile; their compile-only check executes zero tests.

The initial failing fixture mistakenly used absolute journal index as SDK position after suspension; its log remains preserved. The fixture now counts StepRequested/StepCompleted explicitly. A compile-only command initially named the nonexistent `reconciler` directory; its failure is retained, and the corrected `reconcile` command passes. The first fixture compilation also corrected the scheduler seed type. None of these results qualify a real NATS cluster or an OS process fault.

Commands: `go test [ -race ] ./journal -run '^TestGraph' -count=1 -v`, final `-run '^TestGraphCheckpointArchive'`, and `go test ./client ./worker ./reconcile -run '^$'`. Observed source hashes and executed log review are included. This is development component evidence, not independent frozen-source qualification or a new full shared simulation campaign.

## Still required

Native R1/R3 compaction, reopen and collector/fault qualification; worker-triggered compaction with owned metadata, pending-child provenance and continued execution after collection; native Signal/Start/retirement concurrency; malformed v6 cursor and application-level uncertain compaction controls; source-version import/deployment compatibility; history/audit/offline adoption; bounded resource/scale qualification; shared seeded failure/replay integration and all original full/extended/native/matrix/24h/million-drain/release gates. Archive bodies stay owned for audit until lifecycle retirement. Public continuation admission and production online collection remain disabled. The existing frozen 148-family/748-pin full qualification excludes this addition.
