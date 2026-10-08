# Closed older loose temporary files

This batch preserves closed regular files of at least256KiB last modified before2026-10-08, including raw logs, JSON reports and diagnostic binaries. Scripts, current files, existing archives, symlinks, files with pre-existing hardlinks and files observed in use are excluded. Selection, process/descriptor/Docker/mount/loop closure and visibility limits are recorded in [capture.json](capture.json).

Temporary staging uses hardlinks, preserving original bytes without a second data copy. Each archive member is verified against its complete inventory. Local removal requires committed/pushed S3 receipts, fresh full remote byte/member verification, unchanged original inode/bytes and fresh closure checks. Original verdicts are unchanged; this is storage preservation only.
