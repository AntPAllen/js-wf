# Verified copied-root and archive cleanup

Recovered **20.92 GiB** of allocated local storage. Observed filesystem free space rose from **46.33 GiB** to **67.24 GiB**. `/tmp` previously occupied about 47 GiB.

| Archived fixture | Removed locally | Allocated storage recovered |
| --- | --- | ---: |
| `js-wf-bulk-soak-checkpoint8520-copy-20261007.tar.gz` | Copied tree and archive | 9.39 GiB |
| `js-wf-bulk-soak-checkpoint8520-cpu-20261007.tar.gz` | Copied tree and archive | 9.39 GiB |
| `js-wf-bulk-journal-24h-joined-complete-20261006.tar.gz` | Archive only | 1.95 GiB |
| `js-wf-postgres-domain-standalone-50000-20261007.tar.gz` | Archive only | 0.11 GiB |
| `js-wf-raft-safety170-synchronized-durable-20261007.tar.gz` | Archive only | 0.08 GiB |

Both complete copied diagnostic trees were closed and matched every committed inventory byte, mode and mtime before removal. Their systemd services had no main PID, exit 0 and no automatic restart. Every removed archive received a fresh complete S3 compressed-body hash and member-by-member readback against its committed inventory. Visible processes, task descriptors, Docker containers (including stopped containers), mounts and loop devices were checked before deletion; observation limits are recorded. Canonical inventories, receipts and source evidence were already pushed to main. No test verdict changes.

The original failed 24-hour store tree remains local, as do the 400k bounded-preparation and full-latency donor fixtures, million-scale fixtures, other original roots and build caches. The three large original/donor trees account for approximately 14.5 GiB and remain useful for diagnosis. Only copies and redundant staging archives were removed in this pass.

[Removal ledger](removal.json) records exact deleted paths, S3 URLs, full archive hashes, inventories and closure observations. [Executed removal script](executed-removal.py) records the checks. The full CPU profile archive's newly completed S3 receipt was committed before cleanup.

## Restoration

Use the corresponding canonical proof directory and its `s3-readback.json` to download the archive object. Restore to a fresh destination using `scripts/restore-full-fixture-proof.py --archive <downloaded.tar.gz> --metadata <canonical>/archive-verification.json --inventory <canonical>/fixture-inventory.json --destination <fresh-directory>`. This verifies the entire compressed body and all member bytes before restoring, then verifies the restored files and metadata. Native diagnostics must use verified fresh copies.

Canonical proofs:

- [Uninstrumented copied checkpoint](../../bulk-soak-checkpoint8520-copy-2026-10-07/native/s3-readback.json).
- [Profiled copied checkpoint](../../bulk-soak-checkpoint8520-copy-2026-10-07/cpu-profile/s3-readback.json).
- [Original failed 24-hour archive](../../bulk-journal-24h-2026-10-06/terminal/s3-readback.json).
- [Standalone PostgreSQL projection](../../postgres-domain-projection-standalone-50000-2026-10-07/native/s3-readback.json).
- [Full synchronized Raft safety comparison](../../lease-partition-component-2026-10-06/synchronized-safety170/s3-readback.json).
