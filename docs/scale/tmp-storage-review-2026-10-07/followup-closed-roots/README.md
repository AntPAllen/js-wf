# Remaining closed temporary roots

Retired four closed roots: retained block-disk smoke and ten-minute fixtures, full400k latency profiling, and bounded 400k capacity preparation. `/tmp` decreased from approximately 1.8 GiB to 1.3 GiB; the filesystem has approximately 88 GiB available.

Every regular file is preserved in S3 with its path, mode, hash and original nanosecond modification time. The archive member `__original_links_and_worktrees__.json` records symlink targets and clean source checkout commits. Previously offloaded block media is covered by the fifteenth-scale-offload receipts. Restore into a fresh directory using `scripts/fixture_archive.py` and the canonical inventory/hash; recreate listed links deliberately and reconstruct source worktrees at the recorded commit rather than reusing archived `.git` pointers.

Deletion followed committed and pushed metadata/receipts, fresh full remote compressed-body and every-member verification, unchanged local inventories, and process/descriptor/Docker/loop/mount closure checks. Process visibility limitations are recorded. Registered clean worktrees were removed through Git. Temporary archive and hardlink staging copies were also removed. No broker was reopened or test verdict changed.

The full Tier-1 simulation remains active. Candidate inputs, the failed seed-31 diagnostic restore, source checkouts and smaller review evidence remain available for ongoing work. Final measurements are in `remaining-usage.json`; per-root retirement evidence is in `removal.json`.
