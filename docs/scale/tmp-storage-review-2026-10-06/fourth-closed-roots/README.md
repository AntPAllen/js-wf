# Additional closed temporary fixtures

Storage cleanup only. Eight closed historical fixtures are captured as complete regular-file archives with original paths, SHA-256, modes and nanosecond modification times. Archive metadata and inventories are committed and pushed before S3 upload; remote full bodies and all archive members are checked before local deletion.

Only exact archived single-link nested bulk files and files at least 1 MiB are removed. Root logs and small provenance files remain local. Live campaigns, both capacity donors, the original million-timer primary, attachments, caches and system temporary directories are excluded.

Closure checks cover visible process arguments, executables, working directories, thread file descriptors, Docker mounts, loop devices and filesystem mounts. Permission limits are recorded. Historical test verdicts do not change. Restore archived media to a fresh directory before inspection.

Per-root S3 receipts and removed-file manifests record recovery locations. The final summary reports actual allocated bytes reclaimed, excluding newly created archive staging.
