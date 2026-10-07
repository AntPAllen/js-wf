# Failed 24-hour local fixture retired

Reclaimed **3.64 GiB** (3,908,411,392 allocated bytes). `/tmp` now uses approximately **5.4 GiB**, with **85 GiB** free on the filesystem. Both active partition and Tier1 campaigns remained active after cleanup.

The closed failed 24-hour fixture was already fully archived in S3. Before deleting its local root, this pass read the entire remote compressed archive, checked its SHA256 and every member against the committed inventory, checked remote metadata and inventory bytes, and verified the complete local inventory was unchanged. It retained the original failed unit identity and exit status, checked process/descriptor/container/mount/loop closure, and removed the clean source worktree through Git. Permission-limited process observations are recorded in the [ledger](removal.json).

[Complete archive and S3 receipt](../../parallel-recovery-journal-24h-2026-10-07/terminal-failure/s3-readback.json). The original failed verdict is unchanged. Future diagnosis must download this archive and use `scripts/restore-full-fixture-proof.py` with the terminal-failure metadata/inventory and a fresh destination. Historical archived `.git` pointers are not a usable checkout; build from a fresh checkout of the recorded source revision.

The remaining largest data comprises the live partition campaign (about 1.9 GiB), its required input (366 MiB), and retained historical fixtures/source/evidence. The older block-media parents contain absolute symlinks and need a dedicated archival procedure. No blanket temporary-directory deletion was performed.
