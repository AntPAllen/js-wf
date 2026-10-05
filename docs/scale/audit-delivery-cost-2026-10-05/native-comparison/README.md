# Native R3 delivery comparison

At d0a08e6, the normal native test passes6.99s. Each mode rereads the same
100,000 x128B records with exact stream/sequence/payload checks. Checked ACKs and
zero remaining consumers are enforced by the native test.

| Mode | Delivery elapsed | Allocation delta | GC cycles |
| --- | --- | --- | --- |
| Direct Next | 369.681104ms | 266,954,456B | 4 |
| Existing adapter | 380.304660ms | 257,280,080B | 2 |
| Direct Consume | 326.512989ms | 212,249,208B | 2 |
| Next recheck | 361.473646ms | 251,552,264B | 2 |

This single ordered comparison supports investigating callback delivery overhead.
It does not isolate heartbeat cost from other API differences. Linked server
activity is included in allocations; the first path has different cache/GC state.
There is no full400k integrity, fault/cancellation, production adoption or24h pass.

Independent review verifies selected Git source, actual SDK bytes/module metadata
and process closure. Complete1,084-member archive/onepart/24,959,809bytes is read
back, including all original stores and selected source. SDK SHA and archive SHA
are in independent-review.json and archive-verification.json.
