# Closed test binary preservation

The read-only catalogue selected 214 regular, single-link test binaries from visibly closed `/tmp/js-wf-*` roots, excluding both active campaigns, both full400k capacity donors, roots with visible activity and files with additional hard links. Original binary bytes, paths, modes and nanosecond mtimes are recorded in `complete/original-paths.json`; process inspection limits are included.

The complete archive stores 203 unique binary bodies as `<sha256>.test`, plus the exact original-path index. Deduplication changes storage only. Recorded original paths and actual executed binary hashes remain historical evidence. Source, broker stores, logs and provenance records are retained at their original local paths.

The archive and canonical metadata must be committed and fully read back from S3 before removal. The removal script then verifies every remote archive member, remote metadata/inventory, the original mapping, a fresh global visible closure observation and every original binary hash/mode/mtime. See `offload/offload.json` for the completed result; a capture alone does not authorize a claimed removal.

## Fresh restoration

Download the archive from the URL in `complete/s3-readback.json`, and use the committed metadata and inventory with `scripts/restore-full-fixture-proof.py` to restore into a fresh directory. For a recorded original path, find its SHA256 in the restored `original-paths.json`; its exact body is `<sha256>.test`. If making a separate inspection copy, apply that original path record's mode and nanosecond mtime, which can differ between identical binary bodies. Do not overwrite an original run directory. No native test or broker is started by restoration.

This storage operation does not qualify a runtime, source, process lifetime, matrix or 24-hour gate, and does not assert independent provider durability.
