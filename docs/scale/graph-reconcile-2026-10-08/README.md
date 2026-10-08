# Canonical graph repair history — focused verification accepted

Frozen source `6a218b8141a9fbdab9bfe0973a4eebd50231d794`: all 1,496 tracked Go/config/pin inputs matched Git before, remained unchanged after and matched at review. All eight sequential commands passed under original five-minute/count-one/two-Go-CPU/512-MiB limits. No failed/skipped tests or race reports. Raw logs, executed runner/reviewer and complete source inventories are retained here.

| Command | Seconds | Top-level groups |
| --- | ---: | ---: |
| reconcile-normal | 36.119 | 29 |
| journal-normal | 6.677 | 13 |
| worker-normal | 17.086 | 4 |
| sim-normal | 165.804 | 3 |
| reconcile-race | 45.327 | 29 |
| journal-race | 46.341 | 13 |
| worker-race | 79.186 | 4 |
| sim-race | 37.847 | 3 |

The new graph reconciler family completed 100,000 normal and 1,000 race schedules across all 26 modes, with exact replay and actual completed-body accounting. Both simulation commands also passed every one of the 583 unique regression pins and the independent transport census control. All 557 previous pins are byte-for-byte unchanged. Current source inventory: 138 seeded workloads / 583 pins. This campaign extends one family; it does not qualify all 138 workloads.

The complete reconciler package passes all 29 groups in both modes, including the legacy scanner tests. New controls verify rejection of missing configuration, an old terminal signal cache followed by a new invocation generation, exact certified cursor prefixes after failed graph reads, and fenced-loop retry of graph uncertainty/contention/expiry with fail-closed lifecycle/corruption handling. All 13 graph journal groups and four selected worker groups pass normal/race.

Native R1/R3 controls execute actual graph workers: invocation-only publication is repaired by the start scanner; a retained signal without its wakeup resumes a joined replacement worker; an effect remains recorded exactly once; canonical completion is readable after state deletion. Prepared timer/suspended histories independently produce native repair wakeups despite a forged legacy terminal, and canonical terminal history retires an observed generation-bound native timer hint.

## Limits

Modeled repair wakeups and native prepared-timer wakeups deliberately remain pending. Complete model graph drain follows explicit fixture terminal retirement and reader expiry, not automatic production retention. Replacement workers are joined, not process-killed. Native fault/partition/crash/capacity/scale and hosted CI admission are not established by these commands. Graph-configured repair loops are explicit opt-in APIs; CLI/default deployment still requires migration. Invocation/signal publication, fallback-timer/tombstone state, graph-aware purge and child retention, snapshots/continuations/import/history/projections, full current simulation and every original matrix/24-hour/million physical-drain/dependency/default-adoption/release gate remain open. Production collection stays quiescent.

Development failures remain [preserved separately](../graph-reconcile-2026-10-08-development/): draft API mistakes and fixture expectations for lost replies already resolved by readback or invalid empty-history cleanup. No NATS server cause is inferred and no failed verdict is rewritten.
