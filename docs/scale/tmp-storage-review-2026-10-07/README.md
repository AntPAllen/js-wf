# /tmp storage review — 2026-10-07

The earlier pass reclaimed **7.05 GiB**; see [earlier records](seventh-closed-roots/README.md).

Further cleanup: Reclaimed **10.40 GiB** from 284 historical closed directories and 4 redundant archives. Observed free space: **73.64 GiB**.

See [cleanup and restoration details](eighth-closed-roots/README.md), [measured totals](eighth-closed-roots/summary.json), and [archive-copy removal](eighth-verified-staging-removal/removal.json). Live runs and retained scale/causal fixtures remain local.

Earlier cleanup records are in [the October 6 review](../tmp-storage-review-2026-10-06/README.md). No test verdict is changed by storage cleanup.

Latest pass removed another **469.3 MiB** of redundant archive staging copies after fresh complete S3 verification. See [details](ninth-verified-staging-removal/README.md). Observed filesystem free space afterward: approximately **71 GiB**. Original fixture directories remain local.

This pass recovered another **20.92 GiB** from two complete closed diagnostic copies and five redundant archive files after fresh full S3/member verification. Observed filesystem free space: **67.24 GiB**. See [verified removal and restoration records](tenth-copied-roots-and-staging/README.md). Original failed-run stores and donor fixtures remain local.

An implementation experiment later reclaimed its first complete copied diagnostic and staging archive after fresh verified S3/member/inventory/closure checks: **9.39 GiB**. This retires newly created experiment data and is separate from the earlier20.92 GiB cleanup. [Records](../parallel-journal-decode-2026-10-07/first-copy-reclaimed/README.md).

The completed reused-slot experiment was also retired after fresh verified S3/member/inventory/closure checks: **9.39 GiB**. Both decoder experiments now live in S3 with Git receipts. Observed free space: **65.06 GiB**. [Records](../parallel-journal-decode-2026-10-07/reused-copy-reclaimed/README.md).

Latest cleanup retired both completed cold cursor-owner diagnostic copies and staging archives: **18.71 GiB** reclaimed after fresh full S3/member/inventory/closure verification. Observed free space: **64.95 GiB**. [Removal records](eleventh-closed-cursor-copies/README.md). Original failed stores and reusable fixtures remain local.

The new completed closed-watch recovery experiment was also retired after verified S3/member/current-inventory/closure checks: **9.35 GiB** reclaimed. This removes newly created experiment data, separate from the preceding18.71 GiB cleanup. Free space: **64.88 GiB**. [Ledger](../checkpoint8520-cursor-owner-recovery-2026-10-07/closed-watch-reclaimed/README.md).

All three completed new watch-peer diagnostics and staging archives are now retired after fresh complete S3/member/inventory/closure checks: **27.99 GiB** reclaimed, with **64.68 GiB** free. This removes newly created experiment data, separate from earlier passes. [Full ledger](../checkpoint8520-cursor-owner-recovery-2026-10-07/peer-attempts-reclaimed/README.md).

Latest user-requested pass moved two completed original fixtures to verified S3 storage and reclaimed **9.64 GiB**. `/tmp` now uses about **19 GiB**, with **74 GiB** free on the filesystem. [Exact records and restoration](twelfth-completed-originals/README.md).

The three newly completed abrupt-leaf diagnostic fixtures and archives were retired after verified S3/member/local-inventory/closure checks, reclaiming **543.6 MiB**. Live24h and reusable fixtures remain local; approximately **73 GiB** is free. [Exact ledger](../leaf-domain-sigkill-2026-10-07/reclaimed/README.md).

The newly completed leaf/lease-expiry fixture and staging archive were also retired after verified remote/member/current-inventory/closure checks: **185.0 MiB** reclaimed. Approximately **73 GiB** remains free. [Ledger](../leaf-domain-expiry-2026-10-07/reclaimed/README.md).

Latest user-requested pass reclaimed **4.83 GiB** from completed S3-backed diagnostics. `/tmp`: **14.4 GiB**; filesystem free: **77.1 GiB**. Live24h and reusable fixtures remain local. [Exact records and restoration](thirteenth-completed-diagnostics/README.md).

The new completed all-hub/leaf SIGKILL proof fixture and staging archive were also retired after fresh full S3/member/current-inventory/closure checks: **222.7 MiB** reclaimed. [Ledger and restoration](../leaf-all-hub-sigkill-2026-10-07/reclaimed/README.md).

The completed packaged-worker leaf wire fixture and archive are also retired after fresh complete S3/member/current-inventory/closure checks: **359.6 MiB** reclaimed. Collector failure/native pass/independent acceptance remain distinguished. [Ledger and restore](../worker-leaf-wire-2026-10-07/reclaimed/README.md).

Latest user-requested cleanup moved both closed 400k donor stores to verified S3 and retired the completed native blob-boundary fixture, reclaiming **7.80 GiB**. `/tmp`: **7.53 GiB**; filesystem free: **83.75 GiB**. Source worktrees and review records remain local; restore fresh donor stores before reuse. Live24h remained running. [Ledger and restoration](fourteenth-scale-offload/README.md).

Completed blob-boundary seeded campaign fixtures and archives were retired after fresh full S3/member/current-inventory/closure verification, reclaiming **236.1 MiB**. Failed-launch and passing normal100k/race1k results remain distinct. Live24h was retained. [Ledger and restoration](../online-blob-boundary-2026-10-07/seeded-reclaimed/README.md).

Both completed packaged-operator leaf fixtures and staging archives are retired after fresh complete S3/member/current-inventory/closure verification, reclaiming **571.4 MiB**. The first weaker offline measurement and final native acceptance retain separate scopes. Live24h remains local. [Ledger and restoration](../operator-leaf-wire-2026-10-07/reclaimed/README.md).

Latest user-requested pass reclaimed **2.32 GiB** of pre-existing data from closed million stores, block images and 44 loose files after complete remote/member/current-inventory/closure checks. Newly created archive staging was also removed. `/tmp`: **5.69 GiB**; filesystem free: **85.46 GiB**. Original live24h remained active; block-disk source/logs and registered worktrees remain local. [Exact ledger and restoration](fifteenth-scale-offload/README.md).

Three newly completed packaged daemon leaf fixtures/archives were retired after fresh full remote/member/current-inventory/closure verification, reclaiming **677.2 MiB**. Two original readiness failures and the accepted observed run retain separate verdicts in S3; the original live24h remains active. [Ledger and restoration](../operator-daemon-leaf-2026-10-07/reclaimed/README.md).

The newly completed full50000 SQL leaf fixture/archive and exact owned stopped PostgreSQL container/volume were retired after fresh full S3/member/current-inventory/closure and complete closed-volume-media hash verification: **1.24 GiB** reclaimed. Original24h remained active. [Ledger and restoration](../postgres-leaf-projection-50000-2026-10-07/reclaimed/README.md).
