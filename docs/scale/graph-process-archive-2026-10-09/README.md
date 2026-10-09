# Native checkpoint archive process cuts — 2026-10-09

## Executed scope

A shared native fixture now exercises both graceful server restart and real OS process termination for R1 and R3 in the `ARCHIVE` domain. The process test sends `SIGKILL` through `ProcessCluster.KillNode`, verifies each observed wait status is `SIGKILL`, and reopens the original configurations, ports and file stores with new process IDs. The cut occurs after acknowledged first checkpoint compaction while an old reader is pinned.

Fresh native adapters and the public native journal constructor recover an exactly identical logical authority descriptor: head `27`, one unchanged reader, application cursor, publication token and every retained forest. Quorum-witness reads can advance physical stream sequences; those sequences are not logical authority heads. No stores, workflow roots or old reader pins are recreated.

The same lifecycle assertions apply to all four cases: two compactions, preserved absolute records/sequences and complete logical audit, old-reader survival, fresh checkpoint lookup, live-owned append, rejection of archived receipts and v5 schema adoption, collection of original receipts after reader release, and retirement followed by zero physical objects and an independent raw subject census with zero chunk subjects.

| Campaign | Native cases | Result | Package elapsed |
| --- | ---: | --- | ---: |
| Normal | 4 | PASS | 72.198 s |
| Race (`GOMAXPROCS=2`, `GOMEMLIMIT=512MiB`) | 4 | PASS; no race reports | 179.322 s |

Each campaign observed four process exits across its process R1/R3 cases and four exact descriptor recoveries across all cases. `executed-review.py` verifies terminal groups, all four subtests, exit/reopen observations, descriptors, source equality and retained failures. All 934 selected Go/module inputs are unchanged across the successful runs; only the two native fixture files differ from base `72950ea`. No production Go code changed. Exact commands, source hashes, compressed full client traces, server logs and terminal monitoring snapshots are retained here.

## Failures and readiness correction

All three initial failures remain preserved:

1. The exploratory process R3 attempt exhausted the original two-minute fixture context during idempotent compaction after restart. Its earlier fixture version lacked the later exact descriptor assertion and API trace.
2. A traced R3 attempt recovered the identical descriptor but an object metadata API GET received no traced response before the same deadline. While it was stalled, six fresh raw requests—identical metadata GET plus stream Info through each of three endpoints—returned successfully. The exact probe/input/output are retained. This narrows the observed failure to a recovery-time request; it does **not** establish a NATS server defect or distinguish every possible source of reply loss.
3. A later R3 startup failed before the crash cut with API error `10005`, “no suitable peers for placement, peer offline.” A metadata leader and known cluster size alone were insufficient provisioning readiness.

The process fixture now requires all metadata peers current/online before provisioning and after restart, then waits for both original authority/object streams to have recovered leaders and all replicas current before opening adapters. These are bounded read-only observations inside the unchanged two-minute fixture context. No publication is retried by this readiness loop, no production deadline or latency/recovery acceptance target changes, and no missing stream is recreated. Final R3 original-stream readiness took 5.374 s normally and 5.215 s under race after metadata readiness.

Full API tracing after the cut and automatic server/monitoring evidence capture make future failures reviewable. All raw log bytes were verified after gzip compression; `log-manifest.json` retains both raw and compressed hashes.

## CI and remaining work

The graph workflow adds normal/race checkpoint metadata and native process-cut checks with separate retained event files plus server evidence. It also includes the shared `CheckpointCompaction` 1,000-seed family and triggers on its source/pins and the process fixture. That family's test watchdog is 30 minutes, matching the already demonstrated component campaign; production deadlines and the seed CPU watchdog are unchanged. Workflow YAML parses locally. Hosted CI completion is unproven.

This is an acknowledged storage publication followed by whole-cluster process termination. Dispatch/source remains modeled, time/collection is explicitly controlled by the fixture, and process termination leaves the host filesystem running. Worker-process kills, cuts during uncertain compaction publication, compound lease/Start/Signal/retirement/collector faults, storage corruption/power loss, autonomous native collection and SDK recovery, scale/resource limits, import/deployment/offline/public admission and all original qualification/release gates remain open. Complete 149-family normal/race campaigns are still running from frozen `fca8264`; extended campaigns and native fault matrices remain separate requirements.
