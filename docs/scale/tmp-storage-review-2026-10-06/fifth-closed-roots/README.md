# Fifth closed directory cleanup — 2026-10-06

Selected 35 large historical result directories from the current `/tmp` inventory. The live 24-hour campaign, both full 400k capacity donors, million-timer primary, and newest regression/offline diagnostic roots are excluded. One directory containing symlinks was skipped; 34 complete archives were captured without starting any broker or changing a historical verdict.

Each capture records full file paths, bytes, SHA256, modes and original nanosecond mtimes, with local archive-member verification and unchanged-fixture checks. Closure observations include visible processes and descriptors, Docker mounts, loop devices and filesystem mounts; inspection permission limits remain recorded.

The removal stage requires committed and pushed preservation metadata, full remote S3 archive/member verification, matching current local files, fresh closure observations, and single-link regular files. It removes archived nested media/source/evidence and large files, retaining small root logs, provenance and reviewer/model source. Added temporary archive staging is accounted separately from pre-existing reclaimed data.

Each directory's `s3-readback.json` identifies the complete archive. For restoration, download the corresponding `archive_key` using the supplied S3 endpoint and credentials, verify it against `archive-verification.json` and `fixture-inventory.json`, and restore into a fresh directory. Historical stores must be inspected only on verified fresh copies.

`offload.json` and `removed-files.json` record completed local removals. A capture or upload alone does not indicate local removal. `summary.json` records measured totals once removal finishes.
