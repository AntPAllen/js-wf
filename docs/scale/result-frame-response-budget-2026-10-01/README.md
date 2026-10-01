# Result and continuation frame response budgets

Parent source: 8a1e29292b2b9bf5d73b2f7ee9c57798fc48b731. The containing commit records the final implementation. Source hashes and retained logs describe proof scope.

## Production change

Worker-owned result object Put/Get calls now receive separate fifteen-second contexts. The wrapper covers continuation frame storage/verification, spilled step/promise reads and spilled terminal writes, while user effect contexts retain their original lifetime. A custom port's bare DeadlineExceeded is classified as ErrResultBlobUnknown for writes and ErrResultBlobUnavailable for reads so timeout follows normal NAK/release/redelivery rather than a terminal Failed outcome. Existing typed adapter errors retain their classification. Optional result_blob_put/result_blob_get timings measure the port call, not server execution.

The earlier continuation publication budget remains independent. A confirmed frame completion is reused unchanged; a frame stored before completion may be orphaned and a successor can store its newly anchored frame. A step whose result was not durably recorded can execute again, using the same RunOnce key. This preserves at-least-once effects and exactly-once recorded outcomes; it does not invent exactly-once external effects.

## Tier 1

worker_frame_response_budget covers absent frame Put, hidden committed Put, missing verification read before completion and missing replay read after completion. A failed archive publication primes the latter cut. worker_result_response_budget covers absent/hidden step Put, missing completed-step read and absent/hidden terminal Put with actual 921,602-byte payloads. Both run production worker/journal/lease/SDK/client code on shared retained model stores, use a five-minute parent lifetime and require the supplied fifteen-second budget. Virtual time advances 15,000ms per missing response; a replay priming cut adds 1,000ms. Timeout must release ownership, preserve the durable prefix and leave no terminal outcome. Recovery advances epoch and drains; full-history offline replay suppresses effects and preserves result hashes. Repeated unrecorded large-step effects require stable RunOnce keys.

Nine modes are pinned, bringing the corpus to 152. The complete corpus plus both workloads pass race in 194.571s. Vet and all 25 script tests pass. First-ten exact replay and separate-process seed-42 byte identity pass at the 1,000-seed workload gate. The full sim/worker/journal package run passes in 98.876/29.415/0.003s with explicit memory limits. The earlier 100,000-seed attempt was interrupted by the VM reboot and provides no completed seed-count evidence. A fresh bounded-memory campaign is tracked separately; no full release gate is claimed.

## Native R3 contract

All nine real-cluster cases pass under race in 201.13s test / 202.303s package. Withheld result-port responses wait until the actual supplied context expires; hidden-write modes first commit to the real Object Store and the fixture reads back the exact attempted bytes. Absent-write modes require object absence. Read modes use a fixture archive/terminal write error to force completed-frame/step replay. These priming cuts and withheld port replies are not NATS wire faults and do not reproduce the earlier sustained server stalls.

Raw run-entry-to-terminal recovery is 16.343–20.266s, below the unchanged thirty-second gate. Log request_to_terminal is emitted after drain/audit and includes their cost; raw_recovery is captured at terminal result receipt. Ownership remains ErrHeld past the production twelve-second TTL with at least two actual heartbeat renewals. Stored prefixes remain unchanged, confirmed frames are reused, results remain immutable across nodes, duplicate Start is rejected, epoch advances, no lease remains and run stream/consumer drain physically. Each audit finds one invocation/terminal; ordinary spilled-result journals contain four entries, continuation journals nineteen. Effects are two for prefix/suffix and unrecorded step retries, one for completed-step reads and terminal-write repair.

## Controls

The original frame workload rejects inherited request lifetime in 0.003s. A compiled overlay supplies the original unbounded result port: both final model budget assertions fail semantically in 0.013s package, and the native frame-write fixture fails in 3.820s. No build failure or long timeout is used as proof. The model's budget-contract rejection is classified retryably so the negative control reaches its explicit property check immediately. Compiling/running the pinned binary from the repository root failed only because relative testdata was absent; that run is retained and excluded, and the correct package-directory race run supersedes it.

## Remaining scope

Combined frame/result failures with lease expiry, capacity limits, server/process/route faults and online GC remain open. Completed focused workloads do not clear comprehensive final-source simulation, 200 whole-matrix seeds or the five-node 24-hour soak. Historical thirty-second-lease worker-kill smoke misses are configuration evidence; the current production/smoke TTL is twelve seconds and those old misses need no further reproduction.

## Completed bounded-memory 100,000-seed campaign

Both focused workloads passed: frame 112.51s and result 1774.37s. bounded-100k-final.log and bounded-campaign-result.json retain terminal output and binary/source identity from the start record. This supersedes the interrupted attempt for these two workloads; comprehensive final-source coverage and combined fault gates remain open.
