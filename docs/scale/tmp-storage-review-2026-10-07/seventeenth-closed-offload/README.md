# Closed temporary storage moved to S3

This pass preserves 18 closed diagnostic and build directories and 34 closed standalone binaries/logs and four closed archive files, including old test binaries and logs. The standalone files use temporary hardlinks during capture. Original bytes, modes, timestamps and test verdicts are preserved. Active runs, registered worktrees and symlink-bearing directories are recorded as retained in [selection.json](selection.json).

Removal requires committed and pushed inventories, a complete S3 readback, a second full archive and member verification, matching current local files, and fresh process, descriptor, Docker, mount and loop checks. Standalone originals also require matching device, inode and hardlink counts before removal. Only verified directory write permissions may be enabled during removal; archived original modes are preserved.

Use the per-directory `s3-readback.json`, `archive-verification.json` and `fixture-inventory.json` with `scripts/restore-full-fixture-proof.py` to restore into a fresh destination. Restoration starts no process. Empty directories are omitted.

Completed: **1.06 GiB** of pre-existing storage reclaimed in this sweep, plus **0.45 GiB** of newly created archive staging removed. Including the separately verified [completed race retirement](../../tier1-full123-2026-10-07/race-terminal/reclaimed/README.md), total pre-existing storage reclaimed is **1.14 GiB**. `/tmp` now uses **4.61 GiB**, with **85.74 GiB** free on the filesystem. Both original live campaigns and the soak observers remain active. See [removal.json](removal.json) and [final-storage.json](final-storage.json).
