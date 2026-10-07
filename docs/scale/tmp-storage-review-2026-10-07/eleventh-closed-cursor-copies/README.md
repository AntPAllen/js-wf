# Closed cursor recovery copies reclaimed

Removed **18.71 GiB** from the two completed checkpoint8520 cursor-owner diagnostic copies and their redundant local staging archives. Observed filesystem free space afterward: **64.95 GiB**.

Both complete archives, metadata and full inventories are retained in S3 with committed full readback receipts in [the recovery evidence](../../checkpoint8520-cursor-owner-recovery-2026-10-07/README.md). Before removal, a fresh S3 download verified the compressed bytes and every archive member; each local tree matched its complete committed inventory. Process descriptors, executables, arguments, working directories, containers including stopped containers, mounts and loop devices were checked for references. Both services had MainPID0 and Restart=no. The first service's expected exit1 is preserved as a failed test; the second had exit0. Storage cleanup changes no test verdict.

Original failed24h stores, the verified original download archive, 400k donors and million-scale fixtures remain local. Other original roots and caches were not removed in this pass. [Executed removal script](executed-removal.py) and [full removal ledger](removal.json) record both exact paths, S3 URLs, immutable archive hashes, closure observations and allocated-byte totals.

Restore either diagnostic from its S3 receipt's archive URL using the repository's `scripts/fixture_archive.py` verification/restore functions and its committed full inventory. Reopening original failed stores remains unnecessary; use a fresh restore for any further diagnostic.
