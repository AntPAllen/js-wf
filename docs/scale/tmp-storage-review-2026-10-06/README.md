# /tmp storage review — 2026-10-06

Completed guarded S3 offloads recovered **16.61 GiB of older data**. The final observation has **29.11 GiB available** and **69.71 GiB of visible `/tmp` allocations**. Live runs continue writing, so free space changes.

| Completed cleanup | Older allocated bytes recovered |
| --- | ---: |
| Earlier five cardinality fixtures, five closed large-artifact sets and primary rolling archive | 6,122,799,104 |
| Closed final byte-refill stores and actual SDK binaries | 1,118,212,096 |
| 214 closed regular single-link test binaries, 203 unique bodies | 10,588,585,984 |
| Total | 17,829,597,184 |

Temporary archive staging removed separately totals 8,608,985,088 bytes. The disposable exact restoration control is also excluded from old-data recovery.

Every removed original was covered by committed S3 proof metadata and a full remote body readback. Removal additionally checked remote archive members, original hashes/modes/mtimes and fresh visible closure. Process inspection limits are retained. Source, native logs and provenance remain local; binary restoration uses the complete content-addressed archive plus original-path index. See [binary removal](closed-test-binaries/offload/offload.json), [refill removal](byte-refill-final-controls/media-offload/offload.json) and [final inventory](inventory-final/summary.json).

The normal100k simulation and original24h journal campaign remain active. Both full400k capacity donors are retained. User attachments, hidden/system temporary directories, caches and Git storage were not cleared. Remaining closed source/store/proof roots are candidates for a later verified offload, rather than automatic deletion based on their age or filename.

This changes storage only. Historical failed, interrupted and dirty-build verdicts remain unchanged; no native/source/fullmatrix/24h gate or independent S3 durability claim is promoted.
