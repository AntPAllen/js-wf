# Historical disk-stall evidence offload — 2026-10-06

This pass archives all sixteen closed historical block-stall seed ranges and the closed rolling-upgrade seeds 1–13 failure root. Each complete archive preserves current source, logs, original reports, provider downloads and store media. Historical verdicts remain unchanged.

Removal requires committed and pushed metadata, complete S3 upload/readback, a fresh remote archive body and member verification, an unchanged local hash/mode/mtime inventory, and visible process, descriptor, container, mount and loop closure. Inspection permission limits are recorded.

Only single-link files within `raw`, `original-stores`, `originals.zip`, `raw.zip` and `raw.zip.attempt-1` are eligible. Local source, logs and reports remain available. Restore archived evidence into a fresh directory with `scripts/restore-full-fixture-proof.py` and the committed inventory and metadata. Original broker stores are never reopened.

Active campaigns, observers, both full400k donors, attachments, caches and Git storage are retained. See per-root `s3-readback.json` and `offload.json` for transfer and removal records.
