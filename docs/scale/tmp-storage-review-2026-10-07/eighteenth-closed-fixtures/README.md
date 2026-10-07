# Closed `/tmp` fixture offload — 2026-10-07

Retired **347,578,368 allocated bytes (331.5 MiB)** from five closed fixture trees and seven archive staging files. All complete archives, canonical inventories and metadata were uploaded to S3 and read back; immediately before deletion every remote archive member and compressed hash was checked again. Current local inventories and process, descriptor, Docker, mount and loop-device closure were also checked. Hardlinked file storage is counted only when every link was removed.

## Preserved in S3

- Original failed and corrected successful outer-handler lease-expiry race fixtures.
- Three closed copies used to diagnose the million-timer storage/duplicate-target failure. No original verdict was changed and no store was reopened.
- Candidate partition seed 1 native and independent reviewer archives. Their raw local roots remain available for the ongoing full campaign review; only redundant archive staging files were removed.

Each fixture directory contains its complete inventory, archive fingerprint and `s3-readback.json`. Candidate receipts are in [seed001-independent](../../candidate-partition200-2026-10-07/seed001-independent/). The [removal ledger](reclaimed/removal.json) records each deletion and fresh remote verification; [retained snapshot](reclaimed/retained-snapshot.json) records disk usage, inaccessible system directories and unchanged live unit identities.

## Retained locally

The original 24-hour recovery journal test, full 123-workload Tier1 run and 200-seed candidate partition campaign are active. Their stores, captured source/toolchain inputs, executables and candidate input root remain local. Candidate seeds 1–3 have completed natively and seed 4 is running; this is not full-row acceptance. The largest remaining data belongs to the live 24-hour test and continues growing.

The older blockdisk originals contain absolute symlinks and captured block media. They require a dedicated preservation/restore procedure; the regular-file archive helper rejects symlinks. Existing source inputs and donor proof records also remain. No blanket `/tmp` deletion was performed.
