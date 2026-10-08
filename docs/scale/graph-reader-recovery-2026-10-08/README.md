# Canonical reader checkpoint recovery — 2026-10-08

Frozen source: `a0630ca`. Standard local component regression; canonical runtime/release acceptance remains open.

| Verification | Result |
| --- | --- |
| Complete graph package, normal |42 groups,41.149s |
| Complete graph package, race |42 groups,118.592s |
| Shared stream guard, normal/race |1 group each,0.025s/1.036s |
| Recovery shared transport, normal |100,000 schedules,9 modes, exact replay,74.626s |
| Recovery shared transport, race |1,000 schedules,9 modes, exact replay,11.158s |
| Graph pins |All17 publication,12 reader and9 recovery pins pass unchanged in both focused runs |

The2KiB canonical checkpoint carries destination, globally unique reader ID and exact snapshot SHA-256. It carries no graph image, expiry or logical head. Resumption reads canonical authority, checks identity/fingerprint and unexpired lease, and returns independent snapshot/authority copies. It neither creates a pin nor advances the logical head or expiry. Native witnessed reads still advance the physical sequence.

Model controls reject malformed/unknown/duplicate/alias/trailing/oversized encodings before authority access, support256-byte destinations with maximal JSON escaping, reject cancelled/uncertain reads, accept canonical renewal and reject released/expired/changed/foreign handles. Revocation remains effective while bytes exist and when a new reader pins the identical graph. Nine shared modes also test expiry collection during an in-flight read and final complete reclamation, with exact deterministic replay.

Native R1/R3 controls save checkpoint bytes to a file, retire the live graph, discard the original in-memory Reader/coordinator, stop all peers before restarting any, reopen their original stores and fresh adapters, then recover original record/payload bytes from the saved descriptor and witnessed root. Renewal is observed through the old checkpoint; release prevents further resumption; all objects/physical chunks are reclaimed. This is graceful full-store restart with in-process servers, not actual reader-process SIGKILL, power loss or deployment rollout qualification.

`results.json` records original commands/env/exits; complete JSONL/stderr and executed scripts are retained.1,350 selected tracked inputs matched Git before execution and remained unchanged afterward (`source-before.json`, `source-after.json`, `review.json`). The unrelated `.claude-artifacts` directory was retained/excluded. These standard regressions do not independently bind actual SDK executable/dependency/peer-media bytes or establish broad native release acceptance. No official regression body was retried.

Current inventory is129 seeded workloads/471 pins; complete current-source suite qualification remains pending. Canonical destination catalog and every runtime reader/writer/reference path, old-version rollout, compaction/import, abrupt reader/server/process and power loss, complete native fault/concurrency/scale matrices,24h, million physical drain and production online GC remain open. The original full126 normal100k SDK2827904 remained live during terminal review at its original frozen source and300m deadline; these new workloads cannot be credited to it.
