# Full50000 packaged SQL leaf recovery after real SQL startup cancellation

Accepted at recorded clean `b7792d09d3afed9713872bcc1b31bbc9900c3d28`. Actual nonrace SDK 1282641 passes in371.00s body / 371.038s SDK. Original50000/20m case/22m SDK/count1/twoCPU2GiB remain. No production code changed.

The actual packaged startup child 1283670 uses the stock WFEDGE leaf 1283129 and R3 WFVIEW hubs. PostgreSQL backend 100 is demonstrably waiting on a relation lock held by backend 98 during `CREATE INDEX` initialization. SIGTERM joins the child with exit0 within ten seconds while the blocker remains held. Before release, every child SQL session and lock is absent, rows remain zero and neither projection durable exists. Complete joined startup wire contains only the two expected WFVIEW stream-info API publications, 377 client bytes and 3356 server bytes. Full timestamps, actual process birth/argv/executable/SQL application and exact query are retained.

The original three packaged phases then pass unchanged: initial SIGKILL, all50000 results while stopped/100000 lag, partial SQL writer-backend termination plus journal-leader library restart/fatal child exit1, replacement SIGTERM0, zero final lag, all50000 exposed rows/indexed fields identical before/after rebuild, purge and durable-dependency checks. The separate recovery child wire totals 48,327,074 client bytes / 256,554,026 server bytes / 410,106 WFVIEW API publications. These totals exclude the startup child's smaller capture. Leaf-local streams remain zero.

[Independent review](independent-review.json) verifies 2,172 selected Git inputs, 3,360 actual external Go inputs, 1,285 closed SQL-media files, all process and fault identities, every before/after row and the complete 9,219-member archive. [35 actual startup proof/log mutations](startup-controls.json) and [32 inherited recovery mutations](inherited-controls.json) reject. Existing streaming controls pass all three groups including20 captured-wire mutations. All verifications are read-only; no native rerun or original store reopening was used.

Full archive: 205,934,661 bytes, SHA256 `3de9c20e9c60f7566381477c70feb3a8f2017f583168a92db6c6f24171f7d1b2`. Archive/inventory are committed before S3 upload. The complete original source, actual executables, all four full wire files, native logs, original closed NATS stores and complete stopped PostgreSQL media are in this archive. Only the small startup wire is copied into Git; all three original full recovery traces remain in the complete archive.

Manual CI adds `standalone-leaf-startup` with retained systemd producer identity, full original workload, independent review and both proof controls. YAML parses all four rows; this is not hosted acceptance.

This closes this recorded-source SQL relation-lock startup cancellation plus full packaged leaf recovery component. NATS connection/authentication-stage SQL startup, SIGINT-specific SQL startup, natural leaf/route/server faults, process-killed NATS hubs, full matrices, online GC, original million physical drain and full release/24h remain unqualified. The original isolated24h producer, observer and reviewer remain active.

## Restoration

Use `s3-readback.json` to download the complete archive, then `scripts/restore-full-fixture-proof.py` with this directory's archive metadata and inventory into a fresh destination. Empty directories are omitted. Restoration starts no process and changes no original verdict. Local retirement requires fresh remote full-hash/every-member verification, unchanged local inventory, closure checks and exact owned stopped SQL container/volume validation.

The completed local fixture/archive and exact owned stopped SQL container/volume are now retired after fresh verification: [removal proof](../reclaimed/). The complete original bytes remain in S3.
