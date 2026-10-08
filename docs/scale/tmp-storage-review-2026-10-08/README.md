# Temporary storage review — 2026-10-08

Reclaimed **296.9 MiB**. Observable `/tmp` usage is approximately **1.26 GiB**, with **87.3 GiB** free on the filesystem. Protected system directories are excluded from the `/tmp` measurement; limits are recorded.

Removed the completed shared authority witness model/native fixtures, their archives and packaging staging, plus the redundant downloaded candidate component archive. Full S3 compressed bytes and every member were freshly verified against committed/pushed inventories and receipts. Current local inventories/hashes and visible process/descriptor/Docker/mount/loop closure checks preceded deletion. Permission limits are recorded. Complete source, binaries and native media remain restorable from the canonical S3 receipts.

- [Model retirement](../authority-read-witness-tier1-2026-10-08/reclaimed/model/removal.json)
- [Native retirement](../authority-read-witness-tier1-2026-10-08/reclaimed/native/removal.json)
- [Redundant download retirement](redundant-candidate-archive/removal.json)
- [Measured totals](summary.json)

Remaining major roots: extracted candidate inputs (292 MiB), original seed31 diagnostic restore (175 MiB), active full124 normal run (62 MiB), and newly completed full125 race run (70 MiB), whose terminal acceptance and offload review is pending. These are retained for ongoing work. Smaller source checkouts and review/restore records also remain. No additional loose files over10 MiB were found at the top of `/tmp` after cleanup. Test scope and verdicts are unchanged.
