# Historical matrix evidence storage offload — 2026-10-06

This pass preserves five closed historical matrix roots: rolling upgrade seeds66–78 and four clock ranges131–182. Complete current-root archives include raw evidence, store images, provider downloads, source and existing reports. Historical verdicts and implementation gates remain unchanged.

Before removal, each archive and its complete inventory must be committed, pushed, uploaded to S3 and fully read back. A fresh remote compressed-body/member verification, exact current bytes/modes/mtimes census, and visible process/descriptor/container/mount/loop closure then guard removal. Permission limits are recorded in the reports.

Only single-link files within `raw`, `original-stores`, `originals.zip` and `raw.zip`, where present, are candidates for removal. Shared hardlinked binaries are retained; recovery totals exclude their bytes. Source, logs and reports remain local. Restore evidence into a fresh directory with `scripts/restore-full-fixture-proof.py` using the committed metadata and inventory. No original broker store is opened.

Live campaign roots and both full400k capacity donors remain local. Final recovery totals are recorded in `summary.json` after guarded removal.
