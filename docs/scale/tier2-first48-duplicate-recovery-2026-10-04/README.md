# First 48 accepted journal proofs: duplicate recovery

After pushed main `bb424388fe979f9a0d4f14a25adb912ae807ec97` is independently confirmed, all four canonical archives are reconstructed from their retained Git blobs into RAM. Complete archive SHA256/size and every one of their 394 members verify against committed manifests. All 288 expanded raw files also rehash against those same inventories; no open input descriptors exist. All four targets verify before any deletion.

Removing only the accepted raw artifact expansions recovers **2,712,244,224 allocated bytes**. Complete canonical archives are preserved locally and in pushed Git. All model-review reports, source inputs and failed evidence remain unchanged; no broker stores are reopened and no qualification is extended. `recovery.json` records canonical paths, digests, exact byte counts and execution time. Restore raw inputs from the canonical archives before repeating offline review.

`executed-recovery.py` is the exact executed audit, not a general cleanup tool. Its now-removed paths and newly preserved canonical archives deliberately prevent an immediate repeat. The recovered capacity enables review of completed clock shards; it does not solve root-disk capacity for the original full 24-hour soak.
