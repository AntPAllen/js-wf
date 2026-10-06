# Additional closed store offload — 2026-10-06

Seven complete current roots are preserved in S3, with committed inventories and full readback receipts. A second fresh remote compressed-body/member verification, exact local byte/mode/mtime census and visible process/thread-FD/container/mount/loop closure preceded removal.

Removed old media: **3.81 GiB**. Removed temporary archive staging separately: **0.75 GiB**. Two timer cluster directories and their existing receipt ledger, plus five audit store directories, were offloaded. Source records, native logs, metadata and reports remain local. Historical verdicts remain unchanged, including stale timer reports labeled `running`; no successful terminal result is inferred.

Each child directory contains `archive-verification.json`, `fixture-inventory.json`, `s3-readback.json`, and `offload.json`. For inspection, download the referenced full archive and use `scripts/restore-full-fixture-proof.py` with its canonical metadata/inventory and a fresh destination. Previously offloaded SDK binaries use the separate content-addressed binary index.

Both live tests and both full400k capacity fixtures remain local. No original store was reopened. Remaining historical store roots can be considered for another offload with the same checks. No independent provider durability guarantee is claimed.
