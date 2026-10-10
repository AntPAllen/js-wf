# Graph append diagnostics and early reader renewal

Graph append errors now retain preparation/commit phase and absolute entry index, with existing error identities preserved. The native SDK helper records SDK request totals and individual worker append index/kind/duration/errors. Direct parent and child calls now explicitly receive the operation observer and share production-style heartbeat/freshness renewal.

## Verified controls

- A newly simulated three-second acquisition under a four-second TTL causes the prior full-history reader to revoke before its first traversal callback. `slow-acquire-before.log` is the required failure, actual exit1. Renewal before the first traversal restores the previous point-loop contract. The slow-acquisition control,97-vs226 census, empty metadata-only inspection and definite/unknown append controls pass race in2.166s, actual exit0 (`slow-acquire-after.log`). Pin expiration and revoked/unknown outcomes are not relaxed.
- Phase-context-only append controls pass race in1.226s, actual exit0.
- The formerly failed R3 buffered/archive case passes a request-counted diagnostic in67.472s, actual exit0, preserving the child failure, one child call, two parent effects, two parent collection boundaries, confirmed child terminal receipt deletion and43 result. It executes14,934 observed SDK API requests. The original internal append failure remains preserved and its cause remains unconfirmed; this pass is not full current eight-case acceptance.
- The later complete observed matrix loses its tool/process without a terminal receipt. Its command/source snapshot and partial operation log remain unchanged; no exit0 is inferred. `interruption.json` records absence of a live matching process. The complete stopped native stores/build/binary were SHA/member verified and archived to S3; [recovery receipt](../tmp-storage-review-2026-10-10/interrupted-native-observation/removal.json).
- All841 current saved regressions pass normal in8.631s with the renewal fix and error context.

A new frozen complete native matrix remains required. Existing two-minute failed-child archive fixture watchdog, fifteen-second append budget, twelve-second lease TTL and thirty-second kill-to-exact-ACK gate are unchanged. Public continuation admission and production collection stay closed; all broader original plan requirements remain open.
