# Closed temporary storage moved to S3

This pass preserves 18 closed diagnostic and build directories and 34 closed standalone files, including old test binaries and logs. The standalone files use temporary hardlinks during capture. Original bytes, modes, timestamps and test verdicts are preserved. Active runs, registered worktrees and symlink-bearing directories are recorded as retained in [selection.json](selection.json).

Removal requires committed and pushed inventories, a complete S3 readback, a second full archive and member verification, matching current local files, and fresh process, descriptor, Docker, mount and loop checks. Standalone originals also require matching device, inode and hardlink counts before removal. Only verified directory write permissions may be enabled during removal; archived original modes are preserved.

Use the per-directory `s3-readback.json`, `archive-verification.json` and `fixture-inventory.json` with `scripts/restore-full-fixture-proof.py` to restore into a fresh destination. Restoration starts no process. Empty directories are omitted.
