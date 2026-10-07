# Closed `/tmp` evidence batch

Storage preservation of historical closed test directories. Test verdicts and qualification scope are unchanged; no broker or test was started.

The selected directories passed checks for visible process arguments, executables, current directories, descriptors, all Docker mounts including stopped containers, loop devices, mounted filesystems, and existing registered Git worktrees. Permission limits are recorded explicitly in the closure observations. Directories with symlinks or special files were excluded.

A temporary hardlink staging tree preserves file bytes without copying the uncompressed data. The complete compressed archive and every member are verified locally and against its inventory. S3 upload uses a content-addressed key and reads back the entire archive, metadata, and inventory. Removal requires committed provenance and receipt, a fresh complete remote archive/member verification, unchanged local inventories, and fresh closure checks. The staging tree and archive are removed after the originals.

Records:

- `plan.json`: selected and excluded directories, sizes, and initial closure checks.
- `origins.json`: original per-directory inventories and final capture closure checks.
- `fixture-inventory.json`, `archive-verification.json`: complete archive inventory and hash.
- `s3-readback.json`: full remote readback receipt.
- `removal.json`: exact local removals and observed disk space.
- `executed-*.py`: capture and removal tools used for this batch.

Restore into a fresh directory using `scripts/restore-full-fixture-proof.py` and the committed canonical metadata and S3 receipt. Restored paths are grouped by their original `/tmp` directory basename. File bytes, modes, and nanosecond mtimes are preserved; empty directories and ownership metadata are outside the archive format. Restoration does not start a broker.

The active partition campaign, its inputs, registered worktrees, block-device fixtures, and smaller directories remain local.

Completed: **365 original directories**, **1.03 GiB** of original allocated storage reclaimed. The temporary hardlink tree and 200 MiB archive were also removed after verification. `/tmp` was approximately **7.9 GiB**, with **82 GiB** free on the filesystem; the live campaign and its inputs accounted for approximately **6.8 GiB** and continue growing. Exact observations and permission limits are in `remaining-usage.json`.
