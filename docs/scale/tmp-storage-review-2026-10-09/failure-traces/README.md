# Temporary failure traces offloaded

Four closed failure-trace directories were moved from `/tmp` to S3 after complete archive/member verification, full remote SHA-256 readback, unchanged original inventories, and privileged process/container reference checks. Original failure verdicts remain unchanged.

`selection.json` lists original paths; `fixture-inventory.json` preserves file hashes, sizes, modes and mtimes. `s3-readback.json` contains the recovery URL and archive hash. Download that archive with authenticated S3 GET, then restore using `scripts/fixture_archive.py` and the retained verification metadata.

The originals occupied 7,828,059 file bytes. Active tool directories, system sockets, attached plan, registered worktrees and unrelated Claude files were retained. Total `/tmp` usage before cleanup was about 15 MiB; remaining usage is about 7 MiB.
