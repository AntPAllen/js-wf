# Parallel recovery journal: completed ten-minute run

The original journal/seed-1 ten-minute run passed at source `ecb2f895854b68ebdc26530d75c61687d0da7c94`: 93 batches, 2,604 invocations, 28,690 journal entries and 19 confirmed journal-leader faults. All six workload cells passed; all 12,276 bulk latency samples matched point reads. The final bulk stage took 9.348 seconds.

Independent review verified actual SDK and observed stock server identities, source, original row checker, explicit parallel selector, full before/after integrity and complete archived file inventory. This qualifies this single recorded ten-minute row. The 24-hour gate and full release remain open. Full fixture bytes are archived for S3; Git retains evidence and restoration metadata.

Local fixture and staging archive retired after fresh complete S3/member/current-inventory and closure verification. Full bytes remain in the S3 archive referenced by `s3-readback.json`; restore only into a fresh directory. [Removal ledger](../../tmp-storage-review-2026-10-07/twelfth-completed-originals/README.md).
