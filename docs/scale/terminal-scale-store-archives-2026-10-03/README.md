# Completed spilled-input store archival

The accepted terminal scale report at160cf8d matches the retained report bytes.
No open process descriptor referred to its root before archival. All2,833 broker
files under `/tmp/js-wf-scale-spilled-10m-20261002/node-*` were compressed into
`broker-store-archive/`, fully decompressed and SHA256/length checked before
removing the duplicate raw copy. Temporary gzip files were fsynced, atomically
renamed after readback, then recorded in a synced per-file manifest before
removal. Paths, modes and nanosecond mtimes are preserved. Reports/audits/logs,
failed stores and live stores remain untouched.

Original6,408,420,043 bytes compress to1,106,876,537 bytes, freeing5,301,543,506
bytes. The complete compressed broker files remain on this VM; the full original
path/hash/metadata manifest and summary are retained here as lossless gzip.
This is storage preservation, not a new scale qualification or broker restart.

Three actual8,388,600-byte broker blocks, one per node, were restored to a fresh
sample directory using `scripts/restore-fixture-archive.py`; all hashes, lengths,
modes and nanosecond mtimes match. `sample-manifest.json` and
`sample-restore.json` retain this narrower restore proof. To restore the complete
store into a fresh destination:

```sh
python3 scripts/restore-fixture-archive.py \
  --source-root /tmp/js-wf-scale-spilled-10m-20261002 \
  --manifest /tmp/js-wf-scale-spilled-10m-20261002/broker-store-archive/manifest.jsonl \
  --destination /tmp/restored-spilled-scale
```
