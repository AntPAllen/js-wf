# Completed diagnostics moved to S3

Reclaimed **4.83 GiB** from 17 completed diagnostic roots plus the newly completed weak-frame leaf fixture and staging archive. `/tmp` now occupies **14.4 GiB**, with **77.1 GiB** free on the filesystem.

Each removal required fresh complete S3 compressed-hash and archive-member verification, current local inventory matching, and visible process/descriptor/container/mount/loop closure checks. Observation permission limits are recorded. Test verdicts are unchanged.

One read-only source directory interrupted removal. The original verified pending ledger was preserved; the resumed pass verified the complete remote archive and every remaining file before making directories writable and finishing removal. Both executed scripts and logs are retained.

Live24h, reusable 400k donors, million-scale fixtures and registered worktrees remain local.

## Restore

[The ledger](removal.json) records canonical metadata/receipt paths and full archive URLs. Download, verify the compressed hash and every member against the committed inventory, then extract into a fresh directory. The additional leaf fixture has [its own ledger](../../leaf-domain-weak-frame-2026-10-07/reclaimed/removal.json).
