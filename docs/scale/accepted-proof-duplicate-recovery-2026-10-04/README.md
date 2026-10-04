# Accepted proof duplicate recovery

After pushed main `4e226c41d9113ed0531aed0733b33c91a37dfb8f` is independently confirmed, the exact executed script verifies every member of two accepted duplicate expansions, their complete canonical archive SHA256 and every committed Git archive part. Concatenated Git parts match each published complete proof digest. No open input descriptors are found; both targets verify before either deletion.

Only the expanded Tier2 journal 97–108 raw artifact directory and focused clock seed-6 restored directory are removed: **1,025,204,224 allocated bytes** (666,726,400 RAM and 358,477,824 root disk). All 72 raw files and 3,963 clock restoration members match their expected hashes. Compressed local originals and complete pushed proof parts remain; restore from those before repeating offline review. Actual clock workload executables/physical stores remain inside the full canonical archive. Failed originals are unchanged; no stores are reopened or qualification extended.

`recovery.json` records exact targets, byte counts, canonical/Git digests and execution timestamp. `executed-recovery.py` is the exact audit script; it is not a general cleanup tool and its removed paths make an immediate rerun fail. The recovered space supports further shard review; root disk remains too small for the required full 24-hour soak.
