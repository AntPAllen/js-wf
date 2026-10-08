# Second frozen qualification: failed

Source `030b328` passed seven normal commands, ownership race and client race. Reconciler race failed: R1 source-committed Await timed out, and R3 reserved Await returned purged although this fixture performs no purge. Four later race commands were not executed. No data race was reported. Selected source inputs stayed unchanged.

Later instrumented development reproduced the timeout in both replicas: worker Started appends repeatedly lost the shared head CAS against reader acquire/release from bound Start recovery. This confirms contention in those diagnostic runs, not a server defect. The exact cause of the original R3 classification failure remains unproven. Code review independently found two definite Await misclassifications covered by deterministic regressions.

Diagnostic native stores and logs were preserved in the [verified S3 archive](../../tmp-storage-review-2026-10-08/closed-start-repair-artifacts/README.md). This campaign remains failed after subsequent fixes.
