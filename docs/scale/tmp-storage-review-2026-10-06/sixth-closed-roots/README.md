# Further closed temporary fixtures

Reclaimed at least **13.51 GiB of pre-existing allocated storage** from 97 closed directories and 2 redundant archives. Newly created archive staging was removed separately and is excluded from this total. Observed free space: **59.89 GiB**; the live soak continues writing.

Complete inventories were committed and pushed before S3 uploads. Full remote body readback receipts were committed and pushed before deletion. Removal additionally rechecked every remote archive member, metadata, inventory, current local file hash/mode/mtime, and visible process/descriptor/Docker/mount/loop closure. Small root logs/provenance and reviewer/model source remain local. Per-root records identify S3 objects and every removed path.

Hard-link aliases were unlinked without modifying remaining aliases. The reclaimed-byte total counts only observed final-link removals and excludes earlier partial deletions without retained link counts; per-path inode, device, prior link count and allocation are retained in removed-links.json. Full archives contain independent regular-file bodies for every captured path.

The original24h soak, both full400k donors, million-timer primary, latest Raft/candidate evidence, attachments, caches and system temporary directories remain local. Two disk-stall directories containing symlinks were skipped. Failed and interrupted test verdicts remain unchanged.

Restore archives using `scripts/restore-fixture-archive.py` and the committed archive-verification.json and fixture-inventory.json to a fresh destination; native audits must operate on verified restored copies.
