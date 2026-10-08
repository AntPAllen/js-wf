# Temporary storage review — 2026-10-08

Latest requested cleanup reclaimed **701.7 MiB**, including temporary archive copies. Observable `/tmp` usage is now **1.06 GiB**; the filesystem has **87.2 GiB** free. Protected system directories are excluded from the measurement and visibility limits are recorded.

Moved seven closed directories and223 older loose files to S3, then removed local originals, clean registered source worktrees, temporary archives and hardlink staging. This includes the original seed31 diagnostic/media restore, three independent seed review copies, stopped original campaign checkout, completed full126 race fixture, old logs, reports and diagnostic binaries. Complete compressed bytes and every member were freshly verified from S3 against committed/pushed manifests and receipts; unchanged local inventories/inodes and visible process/descriptor/Docker/mount/loop closure checks preceded removal. Original failed artifacts and verdicts are preserved.

- [Closed directories and removal ledger](closed-copies/reclaimed/removal.json)
- [Older loose files and removal ledger](old-loose-files/reclaimed/removal.json)
- [Full126 terminal proof and S3 receipt](../tier1-full126-2026-10-08/race-terminal/)
- [Latest measured totals, remaining roots and live units](latest-summary.json)

The corrected native campaign, full124 normal run and extracted candidate server inputs remain local and in use. Smaller historical source/review directories and operational scripts remain. Both original live supervisors are retained; no tests were restarted for cleanup.

Earlier cleanup separately reclaimed296.9MiB: [original measured totals](summary.json), [model retirement](../authority-read-witness-tier1-2026-10-08/reclaimed/model/removal.json), [native retirement](../authority-read-witness-tier1-2026-10-08/reclaimed/native/removal.json), [redundant download retirement](redundant-candidate-archive/removal.json).
