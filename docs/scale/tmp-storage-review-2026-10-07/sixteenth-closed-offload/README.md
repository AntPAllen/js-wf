# Additional closed temporary directories moved to S3

This pass captures 28 closed directories, including completed diagnostic fixtures, old build directories, and temporary Git object staging. It preserves all regular files, original modes, timestamps, and existing producer results. Storage preservation does not change any implementation verdict.

[Selection ledger](selection.json) records selected and retained roots. Active test and observer roots, registered Git worktrees, and directories containing symlinks remain local. The size scan records permission failures for system managed private directories; those directories are untouched.

Before removal, each archive and inventory is committed and pushed, uploaded to S3, and fully read back. Deletion requires another remote read verifying the complete compressed hash and every member, a matching current local inventory, and fresh process, descriptor, Docker, mount and loop closure checks. Newly created staging archives are also removed after verification.

For restoration, use the chosen directory's `s3-readback.json` archive URL and run `scripts/restore-full-fixture-proof.py` with its `archive-verification.json`, `fixture-inventory.json`, and a fresh destination. Empty directories are omitted by this archive format. Restoration starts no process.

Retirement totals and final live-run checks will be recorded after cleanup.
