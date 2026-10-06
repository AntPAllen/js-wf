# Fifth archive staging cleanup — 2026-10-06

Removed 20 redundant local archive files, reclaiming 1,298,210,816 allocated bytes (1.21 GiB). Original fixture directories were unchanged by this operation.

Every archive was checked against committed metadata and inventory, then downloaded from S3 and verified in full: compressed body, every member, mode, and embedded original mtime inventory. Fresh process, descriptor, Docker mount, loop device, and mount observations preceded each unlink; observer permission limits are retained in the receipts.

`removal.json` maps each removed local path to its committed S3 receipt and archive URL. For restoration, use the corresponding receipt's `archive_key`:

```sh
aws --endpoint-url https://nameless-bird-8772.int.exe.xyz s3 cp s3://nameless-bird-8772/ARCHIVE_KEY /tmp/EXPECTED_ARCHIVE_NAME.tar.gz
```

Supply S3 credentials through the usual environment variables. Verify the downloaded archive against its canonical metadata/inventory before restoring or inspecting historical stores. Full restore must use a fresh directory; historical test verdicts remain unchanged.

The live 24-hour run, both full 400k capacity donors, and the million-timer primary were retained.
