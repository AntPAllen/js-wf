# Canonical graph signal client verification — 2026-10-08

Frozen source `373e6a740a72dfc07f909e1188f233ee99bb78b2` passes all eight count1/five-minute/two-Go-CPU/512MiB commands. All 1,568 selected tracked Go/YAML/module/pin inputs match frozen Git before, after and at review. No source changed during verification.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
| client-normal | 4.708 | 4 |
| journal-normal | 6.792 | 14 |
| worker-normal | 18.859 | 4 |
| sim-normal | 144.442 | 3 |
| client-race | 15.817 | 4 |
| journal-race | 49.558 | 14 |
| worker-race | 92.478 | 4 |
| sim-race | 35.34 | 3 |

## Verified behavior

The complete client package passes normal/race, including all20 prepared native R1/R3 signal cases and earlier canonical terminal controls. Graph lifecycle governs admission despite invalid compatibility state and a forged WF_JRN terminal. RequireRunning validates canonical terminal success/failure/cancellation. Deleted source signals can be confirmed through an exact canonical consumed record and its inline/external owned bytes. Bad consumption hashes, missing owned edges, purge fences and missing retired invocations reject. External native duplicate payloads exceed the ordinary inline signal threshold.

All14 graph journal groups and four selected worker/provenance groups also pass normal/race. Actual R1/R3 graph workers still recover large input/signal/result bytes and child transfer/replay through production purge while preserving effect count one. These are joined replacements, not process kills.

The new signal-client family completes **100,000 normal** and **1,000 race** schedules with exact replay and actual completed-body accounting. All31 modes are observed, including uninitialized histories, canonical terminal outcomes, forged compatibility state/journal, retirement and ID reuse, pre/post-publication invocation replacement or fencing, unknown graph reads/pin outcomes, inline/external duplicate provenance, malformed/unowned/doubled records, source publish reply loss, malformed acknowledgements and foreign retained subjects.

Both simulation commands pass every **642 unique regression fixture**. All611 previous pins remain byte-for-byte unchanged;31 new pins cover the new family. Source inventory is now **140 workloads/642 pins**. CI YAML and its14 family names plus corpus row match the compiled inventory. Hosted CI completion is not inferred.

## Evidence

- [Source/event/inventory review](review.json)
- [Exact commands, environments and timings](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Preserved development failures](../graph-signal-client-2026-10-08-development/)

## Limits and remaining work

Read rechecks are not an atomic publisher/purge fence. An acknowledged signal racing an observed replacement/fence returns its sequence and an error without a wakeup; the signal remains retained. Start/incoming signal publication, external staging and graph-aware worker-created parent notifications remain legacy paths requiring migration. Native and modeled consumed histories are prepared; actual worker regression is separately covered above. Linear scans fail closed on reader expiry/uncertainty. Modeled graph drain uses explicit fixture retirement/expiry; legacy invocation/signal/run data and staging objects remain retained. This is not whole-queue or production online GC qualification.

Complete canonical publisher/consumer/state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment migration, full current140 simulation and all original native reader capacity/concurrency/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent and the full goal remains active.
