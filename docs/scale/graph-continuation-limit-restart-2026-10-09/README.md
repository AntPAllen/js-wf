# Graph continuation SIGKILL plus full persisted-store restart

Three native R3-domain/archive cases pass race on 5c30ebe production code plus
the source-hashed fixture extension. The worker is SIGKILLed after committed
SignalConsumed, its completion or the reserved Failed terminal. All three
JetStream servers then shut down gracefully and reopen their original stores;
every graph/client/state adapter is recreated against the new connections.

| Committed cut | Recovery from worker kill through exact delivery ACK | Old/terminal epoch |
| --- | ---: | --- |
| SignalConsumed at 13 | 16.541699 s | 29 / 33 |
| StepCompleted at 14 | 14.826041 s | 29 / 34 |
| Failed at 15 | 12.210202 s | 30 / 30 |

All three meet the unchanged strict **<30-second** target. Timing includes all
server downtime/reopening, post-restart admission and verification of the
persisted cut, terminal visibility/projection repair, and the exact killed run
sequence's successful higher-delivery ACK (sequence 4, delivery 1 → 2).
The package passes in 153.254 seconds with actual exit zero.

The helper checks all servers are stopped before any restart. Reopened graphs
must reproduce the entire cut prefix before the successor runs. Final journals
preserve every committed cut record, end at exactly 16 entries with Failed at
15, retain `must_not_run` as the rejected request, and never execute its effect.
The initial/middle handlers run once, nonterminal cuts use higher epochs, and an
already committed failure is not rewritten. WF_STATE matches the canonical
failure and fresh clients on every peer agree on the result. TTL remains 12 s
and the dead worker's held lease cannot be prematurely acquired.

[Executed review](executed-review.json) verifies raw prefix/restored/final
records, cut/ACK metadata, timing, handler logs, source hash and actual exits.
The earlier worker-only review now binds its original fixture to published
3d2f889 bytes so extending the helper does not invalidate that historical proof.

These are **graceful server restarts**, because `testcluster.KillNode` calls
Shutdown/WaitForShutdown. They do not qualify abrupt server process death,
VM/power/storage faults, arbitrary partitions, the actual 100,000-entry graph
boundary or the complete current/race/extended suite. Public continuation
admission, production collection and all remaining original broader gates stay
open. The two full simulation campaigns continue on their frozen source cuts.
