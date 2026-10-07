# First diagnostic copy reclaimed

Recovered **9.39 GiB** by removing the complete closed first parallel-decode diagnostic copy and its staging archive after fresh full S3/member readback and current inventory/closure checks. All canonical S3 receipts were pushed before deletion. The follow-up reused-slot diagnostic, original failed24h stores and donor fixtures remain local. No verdict changes.

[Removal ledger](removal.json) records exact paths, remote archive and inventory hashes, visible process/descriptor/Docker/mount/loop checks and service terminal state. Restore from the canonical full S3 object using `scripts/restore-full-fixture-proof.py` and the committed metadata/inventory to a fresh destination before native work.
