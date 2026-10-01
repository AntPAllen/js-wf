# Continuation publication missing-response budget

Parent source: `762df093a3554386e55d4d75855142fe3360ec35`. Final file hashes are in source-sha256.txt; the containing commit records the final implementation.

## Runtime change

Continuation publication shares a fifteen-second context across lease renewal, archive and manifest publication, journal/signal purge, suspension append and handoff. The execute context variable is assigned so existing journal-append closures use the same deadline. Ordinary NAK/release/redelivery repair remains responsible for recovery. Optional operation timing records continuation_publish. Frame Put/Get within the handler remain separate and are outside this publication budget.

## Seeded model

The worker_continuation_response_budget workload uses the production worker, journal, lease, client and SDK on shared retained model transports. A five-minute delivery context distinguishes a real operation budget from the fixture lifetime. Eight cuts cover archive, manifest, journal purge and consumed-signal purge, before commit or after commit with no response. The wrapper requires a deadline within fifteen seconds, advances virtual time by 15,000ms and returns DeadlineExceeded. It does not model Raft or infer a NATS server cause.

The first delivery NAKs, releases ownership and retains the exact frame and recorded prefix without an outcome. A fresh successor repairs/resumes, preserves both ordered signals across compaction, executes each prefix/suffix effect once, advances the fencing epoch, drains and passes raw integrity. Complete-history offline replay returns the original result without rerunning effects. First-ten exact replay, separate-process seed-42 identity and eight pinned modes are required. The original continuation workload's traces are preserved.

The initial 100,000-seed run passes in 148.95s; it precedes the added explicit durable-prefix and absent-outcome assertions. The final stronger workload passes 100,000 seeds in 131.39s, as recorded in final-100k.log. Both use the same production budget.

## Native contract

All eight real R3 transport-port cuts pass under race: 191.80s test / 192.844s package. Each withheld response waits until its actual supplied context expires. Commit-after modes first execute the real underlying store operation. Raw entry-to-terminal recovery is 16.282–16.608s, below the unchanged thirty-second gate. A competitor remains ErrHeld after the original twelve-second TTL and at least two actual heartbeat renewals are required. All cases preserve the stored frame and confirmed prefix, record one timeout, execute two effects total, advance epoch, reject duplicate Start, return immutable results across nodes and physically drain the run stream and consumer. Raw audits find one invocation, one terminal and 18–19 entries.

This fixture withholds a snapshot-port response on real stores. It does not drop a NATS wire reply, reproduce the previous sustained live-owner stall, or establish a server cause.

## Controls and regression checks

A compiled overlay creates but does not use the child context. The model fails its specific archive-budget property (0.053s package); the native archive-drop fixture fails the same property (3.535s package). Both build successfully and do not wait for the delivery lifetime. Overlay source and logs are retained; paths in control-overlay.json identify the original local run and can be remapped for reproduction.

Full sim/worker/journal packages pass in 153.849/29.811/0.005s (before the model's extra prefix assertions). The final 143-pin corpus plus new workload pass race in 22.490s. Vet and all 25 script tests pass. Final focused 100,000-seed verification covers the strengthened model assertions.

## Remaining acceptance scope

Missing frame/result responses during handler execution, further publication/handoff reply and process combinations, continuation limit/TTL combinations, final-source comprehensive simulation, 200 clean whole-matrix seeds and five-node 24-hour soak remain open. The original sustained stall's blocking call and server cause remain unconfirmed. No p99, fencing, journal-capacity or heartbeat gate is relaxed.
