# Closed candidate campaign storage

Complete campaign files from seeds 1–31, including failed seed 31, are archived for S3. This storage review does not independently qualify the campaign or attribute its failure. Source checkout and candidate input remain local; restore all archived campaign files to a fresh directory before review or reuse. The active full deterministic simulation is excluded.

## Completed removal

The closed campaign directory and temporary archive were removed after fresh complete remote archive/member verification, unchanged local inventories, and fresh closure checks. `/tmp` fell from approximately 9.2 GiB to 1.6 GiB; the filesystem has approximately 88 GiB free. Source checkouts, candidate inputs, block-device fixtures, and the running full deterministic simulation remain local. The per-path allocated-block total can count hardlinked files more than once; use the recorded disk usage observations for net space reclaimed. See [removal records](reclaimed/removal.json) and [remaining usage](remaining-usage.json).
