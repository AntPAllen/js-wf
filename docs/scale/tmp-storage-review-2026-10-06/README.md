# /tmp storage review — 2026-10-06

## Latest follow-up

This pass reclaimed at least **13.51 GiB** from 97 additional closed directories and 2 redundant archives after full S3 verification. Observed free space is **59.89 GiB**. See [cleanup records](sixth-closed-roots/README.md) and [measured totals](sixth-closed-roots/summary.json). Hard-linked copies count toward reclaimed space only when the final link is removed.

Previous follow-up: **9.85 GiB of pre-existing allocated data reclaimed** from 34 closed directories and 20 redundant archive copies, after complete S3 preservation and verification. See [previous cleanup](fifth-closed-roots/README.md), [measured totals](fifth-closed-roots/summary.json), and [archive-copy removal](fifth-verified-staging-removal/README.md). Live runs, both full400k donors and the million-timer primary remain local. This follows the [eight-root pass](fourth-closed-roots/summary.json) (2.12 GiB) and [eleven-root pass](further-closed-roots/summary.json) (3.79 GiB).

## Earlier cleanup

Completed guarded S3 offloads now total **33.28 GiB of older data**. This latest pass recovered **4.46 GiB of pre-existing data**, including verified staging copies. The latest observation has **39.64 GiB available**. The live run continues writing, so free space changes. See [latest summary](latest-pass-summary.json), [existing-custody cleanup](closed-fixture-duplicates/README.md) and [six additional historical roots](additional-historical-roots/README.md).

| Completed cleanup | Older allocated bytes recovered |
| --- | ---: |
| Earlier five cardinality fixtures, five closed large-artifact sets and primary rolling archive | 6,122,799,104 |
| Closed final byte-refill stores and actual SDK binaries | 1,118,212,096 |
| 214 closed regular single-link test binaries, 203 unique bodies | 10,588,585,984 |
| Additional seven closed timer/audit store roots | 4,087,672,832 |
| Historical matrix raw evidence and media, five closed roots | 3,157,147,648 |
| Historical disk-stall and upgrade evidence, seventeen closed roots | 4,671,275,008 |
| Closed consumer and partition raw evidence/media, three roots | 1,495,064,576 |
| Further closed fixture source/media, 24 roots | 1,759,395,840 |
| Six further closed historical evidence roots | 2,734,481,408 |
| Total | 35,734,634,496 |

Temporary archive staging removed separately totals 18,576,576,512 bytes. The disposable exact restoration control is also excluded from old-data recovery.

Every removed original was covered by committed S3 proof metadata and a full remote body readback. Removal additionally checked remote archive members, original hashes/modes/mtimes and fresh visible closure. Process inspection limits are retained. Root logs and provenance remain local; selected closed source/media were also offloaded in the latest pass; binary restoration uses the complete content-addressed archive plus original-path index. See [binary removal](closed-test-binaries/offload/offload.json), [refill removal](byte-refill-final-controls/media-offload/offload.json) and [final inventory](inventory-final/summary.json).

The original24h journal campaign remains active. The normal100k simulation and its terminal reviewer have finished; their evidence is retained for review. Both full400k capacity donors are retained. User attachments, hidden/system temporary directories, caches and Git storage were not cleared. Remaining closed source/store/proof roots are candidates for a later verified offload, rather than automatic deletion based on their age or filename.

This changes storage only. Historical failed, interrupted and dirty-build verdicts remain unchanged; no native/source/fullmatrix/24h gate or independent S3 durability claim is promoted.
