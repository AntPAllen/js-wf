# Failed-soak state snapshot: healthy copied-store comparison

Verified copies of the terminal checkpoint1040 campaign's five stores were
reopened with their recorded cluster/server identity. Original store files were
hash-checked against the preserved original archive before copying and checked
again after the diagnostic: no original bytes changed. Only disposable copies
were opened. Clean executed source `886de67`, actual SDK executable/build fields
and all five actual container process executable hashes/build fields retained.

The diagnostic reads a fresh complete KV initial set on each call:

| Variant | Values | Elapsed |
| --- | ---: | ---: |
| Existing 2s per-call / three attempts | 29,177 | 0.131s |
| Overall 20s diagnostic comparison | 29,177 | 0.146s |
| Existing 2s recheck | 29,177 | 0.295s |

All three observed the SDK's complete initial-set barrier. The named diagnostic
passed in14.27s including fixture startup/cleanup. This **does not reproduce**
the live soak's timeout: retained cardinality alone is insufficient to explain
it. No production budget or retry behavior changed. The comparison lacks live
faults and concurrent workflow writes; it cannot establish the historic cause,
a full audit, fault recovery, matrix or24h pass. The million candidate overlaps
on the VM; no isolation claim.

Full copied-store diagnostic proof: 4,190 members / 207,634,286 bytes / 8 parts, all read/hash-verified. SHA256 `151c6eb602e230a7bd26f5cbd1648bceaf3ede64706197f78a9374e6703ae660`.

See [review](independent-review.json), [archive verification](archive-verification.json), and [executed producer](executed-producer.py).
