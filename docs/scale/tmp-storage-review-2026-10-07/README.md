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
