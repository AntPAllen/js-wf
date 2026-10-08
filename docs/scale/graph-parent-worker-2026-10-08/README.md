# Graph worker parent notification verification — 2026-10-08

Frozen source `7e619716b4b3d1fd8132ab0d5ed3b4f04030f9c3` passes all eight count1/five-minute/two-Go-CPU/512MiB commands. Every one of the 1,584 selected tracked Go/YAML/module/pin inputs matches frozen Git before, after and at review. Source remains unchanged throughout verification.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
| client-normal | 4.959 | 5 |
| journal-normal | 9.053 | 14 |
| worker-normal | 26.955 | 5 |
| sim-normal | 100.875 | 4 |
| client-race | 16.565 | 5 |
| journal-race | 48.668 | 14 |
| worker-race | 96.087 | 5 |
| sim-race | 155.604 | 4 |

## Verified behavior

Native and modeled worker constructors bind their notification client to the selected graph store. Existing transports and observer are copied, and the caller's original client stays unchanged. Terminal execution, replay and held/owned duplicates therefore use canonical parent lifecycle and generation checks. Missing-unconfirmed parents, unknown graph metadata and corrupt duplicate consumption prevent ACK. Canonical retirement/replacement permits late-notification suppression. A forged legacy parent tombstone cannot suppress a valid current graph parent.

All22 prepared native R1/R3 worker parent cases pass normal/race: active and uninitialized parents, forged state/journal, purge fence, retirement, missing retired invocation, replacement, unconfirmed missing invocation, and source-deleted inline/external/corrupt consumption. Healthy foreign child lease bytes/revisions remain unchanged and no handler/effect executes. The default native constructor selects graph mode without injecting a graph client. The model adds an uncertain parent root read and checks ACK/NAK, exact notification bytes, retained source-deleted duplicates and explicit graph drain.

The full client package passes normal/race, including original-mode preservation, supplied transport/result binding, incomplete configuration rejection and earlier canonical signal/terminal controls. All14 graph journal groups and five selected worker groups pass. Existing actual R1/R3 child transfer/replay and large input/signal/result worker controls still preserve effect count one and production purge behavior with joined workers.

Both the new parent family and existing terminal-worker family complete **10,000 normal** and **1,000 race** schedules each, with exact replay and actual completed-body accounting. All12 new parent modes and18 old terminal modes are covered. Both simulation commands pass every **654 unique pin**. Inventory is now **141 workloads/654 pins**. CI YAML and its15 family names plus corpus row match the compiled inventory; hosted CI completion is not inferred.

## Existing pin migration

Exactly three prior parent-notification traces have refreshed expected transport events: `parent_notify` (seed4), `parent_notify_drop` (seed20), `parent_notify_lost` (seed15). Constructor binding replaces legacy state reads with canonical parent metadata and invocation rechecks. Workload, seeds, all decision lists, original fault injection and ACK/NAK assertions remain unchanged. Every other639 previous pin remains byte-for-byte unchanged. Twelve new pins cover the new family. The original corpus failure and original pin bytes remain preserved in development logs and Git350a447; no old failure is reclassified as success.

- [Pin hashes and lineage](../graph-parent-worker-2026-10-08-development/pin-migration.json)
- [Complete source/event/inventory review](review.json)
- [Exact commands, environments and timings](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Development failures and limits](../graph-parent-worker-2026-10-08-development/)

## Remaining requirements

Read rechecks are not atomic publisher/purge fencing. Incoming start/signal publication and staging remain legacy; the selected graph runtime must cover parent and child lifecycles. Mixed legacy-parent rollout is not qualified. Native and modeled histories are prepared, actual worker replacements are joined and graph drain follows explicit fixture retirement/expiry. Legacy queue/invocation/staging work remains retained. These controls do not establish process/storage crash or whole-queue drain qualification.

Canonical publication/state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment migration, complete current141 simulation and all original native reader capacity/concurrency/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent; the full goal remains active.
