# Verified duplicate expansion recovery: all-server1–72

Six accepted12-seed shards /432 raw files hash-match their original ZIPs,
canonical proof members and previously pushed archive parts at68ebd26.
Only duplicate raw expansions are removed, freeing3,645,423,616 allocated bytes.
Original ZIPs, canonical proofs, models/source/binaries, failed and live originals
remain. No visible target file descriptors were open before removal. Restore
the raw expansion before replaying models. Acceptance scope is unchanged.

Initial archive verification followed manifest order, causing unnecessary gzip
seeks. The supervisor was stopped to switch to sequential member verification.
That continuation recovered remaining available ranges and then safely stopped
on the assertion for73–84: ranges73–120 had already been recovered in earlier
committed evidence. No new removal occurred in those ranges. Producers/logs and
the scope correction are retained; the final producer covers1–72.
