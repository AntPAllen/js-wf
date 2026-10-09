# Graph repair lease/checkpoint scope

The native graph recovery loops previously delegated to the account-wide `jetStreamLoopPort`, using `system.<kind>-reconciler` and numeric `scan.<kind>` progress. Two graph namespaces could therefore contend for one leader and share progress despite different catalog authorities. The native negative control reproduces the old account-wide lease collision without attributing it to NATS.

Graph repair loops now derive the namespace hash from the GraphStore facade before constructing scanners. It binds authority stream, subject prefix and object bucket. System lease IDs use `graph-repair-<scope>-<kind>-reconciler`; checkpoint keys use `scan.graph-repair.<scope>.<kind>`. Version-1 typed checkpoints embed scope, kind and nonzero Next. Wrong scope/kind/version, malformed state and stale revisions fail closed. All ten supported graph repair kinds use this boundary; legacy repair loops are unchanged. Configuration/scope admission precedes scanner storage access.

## Executed evidence

`results.json` records exact commands and actual exits zero. `race.log` covers all ten kinds in the instrumented KV model, committed lost-ack reload, stale-CAS rejection, and wrong-scope/kind copies. The R1/R3 native boundary fixture holds two different synthetic valid scope hashes simultaneously while the old account-wide leader remains held. It verifies independent progress, old-cursor non-import, fresh adapter recovery after a hidden committed create acknowledgment and cursor CAS. Those fixtures exercise the state boundary, not two automatically routed graph deployments. Package time: 3.665 seconds.

`admission-race.log` verifies invalid native graph configuration produces zero instrumented KV calls and rejects nil JetStream. Package time: 1.013 seconds. `native-loops-race.log` exercises actual GraphStore-derived scope and production canonical pending-Start and terminal recovery loops, including present-projection audits on R1/R3. They retain existing deadlines, durable-cut recovery, repeated deleted/corrupt projection repair, unchanged canonical journal and effect-count assertions. Package time: 51.339 seconds.

`review-inputs.json` records three directly relevant sources during review. It is not a complete compiled-input provenance manifest. The generic loop and all 155 seeded families/835 registered traces are unchanged; this change has no new seeded-family claim.

## Upgrade and remaining scope

Existing unscoped scan checkpoints are not automatically imported; new scoped loops begin at sequence one and rescan. Previous graph workers use different leases, so old and new versions do not coordinate leadership. Upgrade graph repair workers after stopping their previous unscoped loops. Full migration/rolling adoption remains unqualified.

This isolates repair leadership and progress. It does not establish namespace-aware ownership/routing for the shared invocation, timer, signal and dispatch stores. Account/domain boundaries remain those of the selected JetStream instance. Full multi-namespace worker deployment, arbitrary fault interleaving, process/VM/storage/route faults, clock/scale/retention/purge/collector rollout and every original broader requirement remain open. Both live frozen race campaigns and queued frozen full155 normal exclude this later scope change. Full current normal/race/all-pin/extended qualification remains open; production collection stays off.
