# Child ownership and signal provenance — focused verification accepted

Frozen source `31f8e04edb04edbb581d2eb1a852f7bbda7800d2`: all 1,465 tracked Go/config/pin inputs matched Git before execution, remained unchanged after execution and matched during review. Ten sequential commands passed with their original five-minute/count-one/two-Go-CPU/512-MiB limits. No failed/skipped tests or race reports. Raw logs, executed runner/reviewer and complete source inventories are retained here.

| Command | Seconds | Top-level groups |
| --- | ---: | ---: |
| worker-normal | 20.677 | 4 |
| wf-normal | 1.18 | 59 |
| client-normal | 3.172 | 3 |
| child-sim-normal | 211.099 | 3 |
| signal-sim-normal | 86.795 | 2 |
| worker-race | 76.141 | 4 |
| wf-race | 12.222 | 59 |
| client-race | 7.769 | 3 |
| child-sim-race | 226.251 | 3 |
| signal-sim-race | 118.883 | 2 |

Both new model families completed 10,000 normal and 1,000 race schedules with exact replay. The transfer family also ran all 557 unique pins in both modes; all 553 previous pins are byte-for-byte unchanged. Current inventory: 137 seeded workloads / 557 pins. The transfer runner admits at most two independent schedules simultaneously, checks results in seed order and joins both runners; it does not change model events, pins or deadlines. This focused campaign does not qualify all 137 workloads.

Native R1/R3 controls execute real sync/async child workflows with external results or external signal staging (eight scenarios), collect original child bytes, replace joined workers and replay the parent without the source child. Eighteen forgery and twelve selected-signal cases reject invalid provenance. Complete workflow SDK normal/race packages and canonical client controls also pass.

Development identified a runtime bug: an ordinary signal arriving before a child request could later be selected as that child's result. The SDK's optional validator now checks the exact selected signal against durable runtime child metadata; ordinary AwaitSignal behavior remains available. New early-signal controls reject fake inline outcomes and references to otherwise valid parent-owned bytes before effects execute.

The previous frozen attempt at c1cf238 remains failed at its original 300.010-second deadline; the diagnostic stack seed is not a certified completed prefix. Development budget/fixture failures and a discarded cache optimization remain preserved separately. No server cause is inferred or failed verdict rewritten.

## Scope limits

Modeled child terminals are prepared fixtures; child dispatch remains pending. Native replacement workers are joined, not process kills. Complete graph drain follows explicit fixture lifecycle cleanup, not automatic production purge coordination. Production child retention must preserve the source generation until the parent's ownership publication. Other canonical reference/reader/writer migration, full current simulation, native crash/partition/capacity/scale and every original matrix/24-hour/million physical-drain/dependency/default-adoption/release gate remain open. Production collection stays quiescent.
