# Completed original fixtures moved to S3

Reclaimed **9.64 GiB** (10,354,069,504 allocated bytes) from the completed failed 24-hour fixture and completed accepted ten-minute fixture, plus their local archive copies. Approximately **74 GiB** is now free; `/tmp` is approximately **19 GiB**.

Each removal followed a fresh complete remote compressed-body and every-member verification, exact local byte/mode/mtime inventory match, and process/descriptor/Docker/mount/loop closure checks. Registered source worktrees were removed through Git only after verification. Failed and accepted verdicts are unchanged. Concurrent per-item free-space observations overlap; use the final combined summary for free space.

Restoration sources, complete inventories, archive hashes and S3 readback receipts:

- [Failed 24-hour original](../../bulk-journal-24h-2026-10-06/terminal/).
- [Accepted ten-minute original](../../parallel-recovery-journal-ten-minute-2026-10-07/terminal/).

Download the receipt's archive URL with S3 credentials and restore to a fresh directory using `scripts/fixture_archive.py:restore`, supplying committed archive metadata and inventory. The archived worktree `.git` pointer is historical; reconstruct source from the recorded Git revision before any new execution. Do not resume either archived original store.

The 400k donor fixtures and million-scale fixtures remain local for planned tests. Nothing was removed based only on age or filename. Exact checks and executed deletion scripts are retained in the two subdirectories.
