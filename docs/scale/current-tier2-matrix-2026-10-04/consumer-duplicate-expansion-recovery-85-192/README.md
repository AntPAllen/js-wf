# Verified duplicate expansion recovery: consumer85–192

Nine accepted12-seed shards /648 raw files hash-match their original ZIPs,
every canonical proof member and previously pushed archive parts at68ebd26.
Canonical archives are checked sequentially to avoid repeated gzip seeks.
Only duplicate raw expansions are removed. Exact allocated bytes recovered are
recorded in `recovery.json`. Original ZIPs/canonical proofs, models/source/binaries
and failed/live originals remain; no visible target descriptors were open.
Restore raw expansions before model replay. Qualification scope is unchanged.
