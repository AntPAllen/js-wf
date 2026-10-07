# Completed peer-watch attempts retired

Reclaimed **27.99 GiB** from the unadmitted collection, failed cleanup-target and accepted native peer-outage copied diagnostics plus their redundant staging archives. Observed free space: **64.68 GiB**.

All three complete archives, metadata and inventories have committed S3 full readback receipts. Before deletion, a fresh complete S3 download verifies each compressed byte hash and every member; the complete local tree still matches its committed inventory. Each service is MainPID0/Restart=no with its original expected status: exit1 for the first two and exit0 for the accepted run. Visible process descriptors/executables/arguments/cwd, all Docker containers including stopped ones, mounts and loops are checked. [Executed script](executed-removal.py) and [removal ledger](removal.json) record exact paths, S3 URLs, closure observations and allocated-byte totals.

The unadmitted and failed verdicts are preserved. Original failed24h stores, donor/million fixtures and the verified original download remain local. Restoration uses each committed S3 receipt and full inventory into a fresh directory; no original failed stores were reopened.
