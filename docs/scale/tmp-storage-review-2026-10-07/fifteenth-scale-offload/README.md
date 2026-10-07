# Closed temporary stores and loose files moved to S3

This pass preserves the closed legacy million-capacity fixture, two closed block-media images, and 44 closed loose files from `/tmp`. The original test results and artifact bytes are unchanged. Archive manifests are committed before upload; S3 receipts bind full compressed-byte readback. Removal requires a second remote read with every archive member verified against the committed inventory, unchanged local bytes, and fresh process/descriptor/Docker/mount/loop closure checks.

The million-timer primary root already has a complete committed archive and receipt under [complete-primary-root](../../million-timer-terminal-2026-10-02/complete-primary-root-2026-10-06/). It is removed only after its complete current inventory and remote archive are verified again.

The block-disk parent roots, their source, test/review logs, other node stores and original `node-2` symlinks remain local. Only the closed `wf-block-*` directories containing the unmounted backing images are moved. Parent `BLOCK_MEDIA_MOVED_TO_S3.json` records explain restoration. The regular-file archive format omits empty directories: recreate the empty `store` directory after restoring an image, before using the documented block fixture setup. No fixture or executable is started by cleanup or restoration.

The loose-file archive contains original filenames at its root. The complete [origin and closure ledger](top-level-files/origins-and-closure.json) records each selected file. These include old compiled test binaries, downloaded artifacts and reports. Staging used hardlinks, preserving file bytes and metadata without duplicating the originals' allocated storage. Original files and temporary hardlinks are deleted only after verification. Unselected files, registered source worktrees and the active 24-hour run remain local.

## Restoration

Choose an archive's `s3-readback.json`, download its `archive.url` using the standard authenticated S3 client, and restore into a **fresh directory**:

```sh
python3 scripts/restore-full-fixture-proof.py \
  --archive /path/to/downloaded-proof.tar.gz \
  --metadata docs/scale/tmp-storage-review-2026-10-07/fifteenth-scale-offload/CHOICE/archive-verification.json \
  --inventory docs/scale/tmp-storage-review-2026-10-07/fifteenth-scale-offload/CHOICE/fixture-inventory.json \
  --destination /tmp/fresh-restored-CHOICE
```

`CHOICE` is `million-capacity`, `blockdisk-smoke`, `blockdisk-ten-minute` or `top-level-files`. The restore command verifies the complete compressed archive and every file before restoration. For the million-timer root, use the metadata and inventory in its existing complete-primary-root directory. Restoring a fixture does not qualify an implementation gate or change the original verdict.
