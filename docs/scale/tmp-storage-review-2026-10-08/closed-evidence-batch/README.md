# Closed temporary evidence batch

Storage preservation of 301 closed evidence directories and all 4,164 current regular files. Historical accepted, failed and partial test results keep their original scope and verdict. No native store was opened. Live roots, Git worktrees, directories with symlinks or special files, small directories and unrelated VM data were excluded. Process, descriptor, Docker (including stopped containers), mount and loop checks and their permission limits are recorded in `capture.json`.

`fixture-inventory.json` preserves every relative path, byte hash, size, mode and original nanosecond modification time. `archive-verification.json` identifies the full compressed archive. The forthcoming S3 receipt records a complete remote byte readback; local originals remain until that receipt is committed and pushed and a fresh remote member verification and closure check pass.

To restore after removal, download the archive URL in `s3-readback.json` using the authorized S3 credentials. Verify its complete compressed SHA-256 and members against the committed metadata and inventory using `scripts/fixture_archive.py:restore`, specifying a fresh destination. Each archived top-level directory has its original `/tmp` basename. Restore files for inspection; do not automatically execute restored programs or reopen native stores.
