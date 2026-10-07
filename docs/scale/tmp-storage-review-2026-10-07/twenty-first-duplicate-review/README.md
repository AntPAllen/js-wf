# Duplicate temporary storage cleanup

Reclaimed **1,025,929,216 allocated bytes (978 MiB)** by removing the obsolete 24-hour archive and its duplicate observer directory. Every one of the old archive’s 10,453 files and all observer files exactly matched the final canonical archive inventory. A fresh complete S3 readback verified every member and the entire compressed body before deletion. Process, descriptor, Docker mount, loop-device and filesystem mount checks are recorded, including permission limits.

The original failed 24-hour verdict is unchanged. Both running campaigns retain their original process and invocation identities. The remaining large campaign root and its input binary are required until terminal review; retained block-media fixture parents and historical source/review files remain available.

See [cleanup.json](cleanup.json) for verification and deletion evidence and [final-storage.json](final-storage.json) for final usage and live units. Canonical S3 restoration metadata remains under [terminal-failure](../../parallel-recovery-journal-24h-2026-10-07/terminal-failure/README.md).
