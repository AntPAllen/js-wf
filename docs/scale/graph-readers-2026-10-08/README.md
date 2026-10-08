# Durable reader retention component — 2026-10-08

Source: `7a9ea04c82cb139f2d971ceec2723778bee4f0eb`. This is standard local regression of the isolated experimental protocol. It does not qualify canonical runtime migration or release acceptance.

| Verification | Result |
| --- | --- |
| Complete graph package, normal |38 top-level groups,27.856s |
| Complete graph package, race |38 top-level groups,113.461s |
| Shared stream guard, normal/race |1 group each,0.025s/1.035s |
| Reader seeded transport, normal |100,000 schedules,12 modes, exact replay,101.529s |
| Reader seeded transport, race |1,000 schedules,12 modes, exact replay,13.938s |
| Publication/reader pins |All17 existing publication and12 new reader pins replay unchanged in both focused runs |

Focused runs also cover independent raw receipt census/copy/fault controls. The seeded tests verify retained record/payload identity after retirement, both acquisition/retirement and renewal/collector winners, explicit release/expiry, unknown mutation acknowledgements and uncertain ancestry. Final collection requires no live graph, reader pins or objects. Unit controls additionally cover bounded slots, independent snapshots, original-head append fencing, missing ancestry, revoked renewal and empty-snapshot expiry.

Native R1/R3 reader controls pass in both complete package runs. They append/reuse an exact original payload, retain a snapshot through retirement and adapter reopen, extend its lease, then expire/reclaim all objects and physical chunks. Permanent schema and head fences remain. This reopen keeps the same running servers; it is not server restart, process-kill or power-loss reader qualification. Reader handle persistence/resumption and those crash tests remain required.

`results.json` records original commands, environments, exits and elapsed package results; the complete JSONL/stderr files retain native results. `source-before.json` binds1,339 selected tracked inputs to Git before execution; `source-after.json` and `review.json` confirm those selected bytes stayed unchanged. Unrelated `.claude-artifacts` files were retained and excluded from the input inventory. `executed-regression.py` and `executed-review.py` preserve the runner and terminal review. This evidence does not independently bind actual SDK executable bytes, dependency/peer media or deployment rollout. Earlier development compile/helper-name and pin-directory setup errors were corrected before the frozen regression; a review filename-suffix correction did not repeat any test body.

Root snapshots are bounded to8 pins/256KiB. The authority envelope ceiling is260KiB; individual grant fences remain32KiB. `AcquireReader` captures only the current canonical live graph. The permanent retention schema prevents v1 retirement/downgrade after acquisition. `RetireLive` preserves pins. Collection first acknowledges an expiry CAS before treating an expired pin as absent; a lost expiry reply stops deletion. `ExpireReaders` also handles empty snapshots at known destinations. Clients must finish reads before expiry or renew using a collection-consistent clock.

Current source adds a128th seeded workload and12 pins, for462 pins. Complete current-source128-suite qualification, old deployed adapter rejection/rollout, destination catalog integration, partial compaction/import, every canonical reader/writer/runtime path, complete native fault/concurrency/scale matrices,24h, million physical drain and production online GC remain open. The older full126 normal100k SDK2827904 was still live at terminal review under its original frozen source/deadline; it cannot qualify these new changes.
