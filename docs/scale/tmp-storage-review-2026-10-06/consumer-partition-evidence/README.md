# Closed consumer and partition evidence storage

Completed verified S3 offloads recovered **1.39 GiB of historical raw evidence and store media** from three closed roots. Complete archives include every original regular file; committed inventories and receipts allow fresh restoration. Source, logs, reports and executable inputs remain local.

Before removing originals, the executed offload script re-read every S3 archive member and full compressed body, verified current local hashes, modes and mtimes, and checked visible processes, working directories, descriptors, containers, mounts and loop devices. Process inspection limits are recorded. Single-link files were removed only from the explicit raw/media paths. No broker or native test was reopened.

Separately, [staging removal](../verified-staging-removal/removal.json) removed eighteen older local archive copies only after fresh complete remote archive/member checks and closure. Including the three new upload archives, **1.07 GiB of staging** was removed. This pass recovered **2.47 GiB total**; [summary](summary.json) records the observed free space.

The original 24h journal and hosted campaign observer remain active. Both full400k donors, user attachments, hidden/system temporary directories, caches and Git storage remain untouched. Historical success/failure scopes are unchanged; this does not qualify a new runtime gate. Future store audits require a fresh verified S3 restoration.
