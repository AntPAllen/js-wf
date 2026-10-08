# Remaining temporary evidence cleanup

This batch preserves 3,989 inactive temporary entries and 5,380 regular files, including closed test evidence and an abandoned Go build directory. It preserves original bytes, paths, modes and nanosecond modification times. Test verdicts are unchanged. Registered Git worktrees, live simulation data, active supervisor output, and unrelated VM/application files were retained.

`capture.json` records selection and privileged process, descriptor, container, mount and loop checks. `fixture-inventory.json` lists all files; `archive-verification.json` identifies the fully checked archive. Local removal requires a committed and pushed S3 receipt, fresh complete remote archive/member verification, unchanged originals and fresh privileged closure with no permission gaps.

Restore by downloading the archive URL recorded in `s3-readback.json`, checking its complete compressed hash and inventory, and using `scripts/fixture_archive.py:restore` into a fresh directory. Archived top-level names match their original `/tmp` names. Do not automatically execute restored programs or open restored native stores.

## Local retirement completed

All 3,989 selected original entries and the local transfer archive were removed after the receipt was committed and pushed, fresh complete remote compressed-byte/member verification, unchanged original inventories and a privileged process/descriptor/container/mount/loop check with no permission gaps. The removal report records 154886144 allocated bytes including the temporary transfer archive. S3 is the canonical archive; Git retains its complete inventory and recovery receipt.
