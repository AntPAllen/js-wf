# Complete archive restoration control

`scripts/restore-full-fixture-proof.py` restored all 5,982 files from the complete byte-refill archive to a fresh disposable directory. Every byte, regular-file mode and original nanosecond mtime matched the canonical inventory. No broker or native test was started, and the historical fixture was not reopened.

Six archive controls pass, covering corruption, traversal, duplicate inventory, symlinks, full compressed-body hashes, exact restoration, preflight rejection without creating a destination, existing-directory protection and archive path replacement during restoration.

To restore a downloaded complete proof:

```sh
python3 scripts/restore-full-fixture-proof.py --archive proof.tar.gz \
  --metadata archive-verification.json --inventory fixture-inventory.json \
  --destination /tmp/fresh-fixture
```

Use the committed canonical metadata and inventory. This restores files only; it does not qualify a runtime gate. Older per-file gzip archives require the separate legacy decompression helper after this stage.
