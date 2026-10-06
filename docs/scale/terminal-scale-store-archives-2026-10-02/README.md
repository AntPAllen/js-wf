# Lossless archival of completed capacity stores

Three completed scale roots now retain broker files as gzip archives under
`broker-store-archive/`. Reports, live-cohort audits and node logs remain in
place. Before archival, reports required status completed and no process file
descriptor referred to the root. Every original store file was compressed,
fully decompressed and compared by length/SHA256 before its original was
removed. The synced JSONL manifest preserves paths, lengths, hashes, modes and
mtime. Summaries and all original-path manifests are retained here; exact file counts
and byte totals are in summaries.json. The gzip broker files remain on the VM.

Restore a complete broker store to a new directory:

```sh
python3 scripts/restore-fixture-archive.py \
  --source-root /tmp/js-wf-scale-live-inline-10m-20260930 \
  --manifest /tmp/js-wf-scale-live-inline-10m-20260930/broker-store-archive/manifest.jsonl \
  --destination /tmp/restored-scale-inline-10m
```

The restore command verifies every output hash and preserves mode/mtime; it
refuses an existing destination. Copy retained report/audit/log files separately
if the restored root will be used by the offline report verifier. Unit controls
cover bytes/metadata, corruption and overwrite refusal. One actual large broker
block from each first archived root was restored and hash-verified. Archival
verified every file; the sample restores do not constitute a full broker restart.
Live million-timer files were excluded. This recovered capacity for the new10M
spilled-input run without discarding the prior measurements or broker bytes.

## Storage update — 2026-10-06

The closed cardinality fixture broker gzip files have moved to complete verified
S3 archives. The historical local paths above now retain reports and restore
ledgers. See [complete per-fixture archive/readback/offload records](../closed-scale-s3-2026-10-06/).
For a future restore, download and verify that fixture's full archive against its
committed compressed hash and member inventory, unpack it into a fresh root, then
use the retained legacy manifest to restore gzip broker members into a second
fresh destination. Preserve original modes/nanosecond mtimes from the manifest.
No original closed stores were reopened during offload.
