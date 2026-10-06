# /tmp storage review — 2026-10-06

Completed guarded S3 offloads recovered **20.41 GiB of older data**, including **2.94 GiB** in the newest five-root historical matrix pass. The latest observation has **33.54 GiB available**. Live runs continue writing, so free space changes. See [historical matrix evidence offload](historical-matrix-evidence/README.md) and its summary for the newest observation.

| Completed cleanup | Older allocated bytes recovered |
| --- | ---: |
| Earlier five cardinality fixtures, five closed large-artifact sets and primary rolling archive | 6,122,799,104 |
| Closed final byte-refill stores and actual SDK binaries | 1,118,212,096 |
| 214 closed regular single-link test binaries, 203 unique bodies | 10,588,585,984 |
| Additional seven closed timer/audit store roots | 4,087,672,832 |
| Historical matrix raw evidence and media, five closed roots | 3,157,147,648 |
| Total | 25,074,417,664 |

Temporary archive staging removed separately totals 14,783,348,736 bytes. The disposable exact restoration control is also excluded from old-data recovery.

Every removed original was covered by committed S3 proof metadata and a full remote body readback. Removal additionally checked remote archive members, original hashes/modes/mtimes and fresh visible closure. Process inspection limits are retained. Source, native logs and provenance remain local; binary restoration uses the complete content-addressed archive plus original-path index. See [binary removal](closed-test-binaries/offload/offload.json), [refill removal](byte-refill-final-controls/media-offload/offload.json) and [final inventory](inventory-final/summary.json).

The original24h journal campaign remains active. The normal100k simulation and its terminal reviewer have finished; their evidence is retained for review. Both full400k capacity donors are retained. User attachments, hidden/system temporary directories, caches and Git storage were not cleared. Remaining closed source/store/proof roots are candidates for a later verified offload, rather than automatic deletion based on their age or filename.

This changes storage only. Historical failed, interrupted and dirty-build verdicts remain unchanged; no native/source/fullmatrix/24h gate or independent S3 durability claim is promoted.
