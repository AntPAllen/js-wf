# Corrected Signal actor focused race acceptance

Frozen `cc8363da0f99dafbaff6eec454b85cc00fa62a63` passes all 1,000 contiguous actor bodies and exact replays in 3,740.43 seconds, directed caller-contention seed 926 in 27.42 seconds, and its registered retry pin. Test package elapsed is 3,768.886 seconds; both actual child commands exit zero. Raw events and pin output, the two completed command records and retained binary provenance are copied here. Interrupted predecessor attempts remain unqualified.

`review.py` independently parses completed-body/pass/fail events, checks the actual command exits and working directory, verifies the retained binary hash and actual race build information, and matches all 1,755 frozen source/module/trace inputs against disk and pinned Git objects. `executed-review.json` records the successful review execution. `review-result.json` records timings and raw evidence hashes. The same corrected source already has separately accepted focused normal evidence in `../complete-normal/`.

This qualifies the focused corrected actor workload, directed contention control and caller-retry pin only. It excludes later expiry/reader-maintenance/audit/projection/CLI code and does not qualify the complete current suite, all current traces, extended seeds, arbitrary operation permutations or any original native/runtime/fault/scale/soak/drain/migration/adoption/release requirement.

The supervising service proceeded to its queued retained expiry race after these commands succeeded. That phase has no terminal verdict yet; the live service is not itself an acceptance result for the expiry workload.
