# Graph continuation recovery after worker and all-server SIGKILL

The process-cluster fixture kills the worker at three committed near-limit
boundaries, then SIGKILLs **all three separate NATS server processes before
restarting any of them**. Kernel WaitStatus must report signal 9 for each server;
replacement PIDs must differ. Servers reopen the original file stores and every
graph/client/state adapter is reconstructed from the new connections.

| Durable cut | Kill → restored cut, terminal/projection and exact ACK | Old/terminal epoch |
| --- | ---: | --- |
| SignalConsumed at 13 | 17.002235 s | 28 / 32 |
| StepCompleted at 14 | 15.766871 s | 28 / 33 |
| Failed at 15 | 13.394101 s | 28 / 28 |

All three pass the unchanged **strict <30 s** recovery gate, including server
death/reopening and successful ACK of the exact killed run sequence at a higher
delivery count (4/1 → 4/2). The package passes in 127.416 s, actual exit zero.
The worker/controller run with the Go race detector; external servers use the
normal module-pinned server build, whose identical SHA/build metadata across
all cuts is retained in [server-binaries.json](server-binaries.json).

Every restored cut matches its committed prefix before the successor starts.
Final canonical records preserve that prefix, stay at exactly 16 test-budget
entries, retain the rejected `must_not_run` declaration and never run its effect.
Initial/middle stages do not repeat; nonterminal cuts complete under higher
epochs, while already-terminal history keeps its original epoch and bytes.
WF_STATE and results from fresh clients on all peers agree. The active dead
owner lease cannot be acquired prematurely and its TTL remains 12 seconds.

[Executed review](executed-review.json) verifies the raw journal/prefix/restore
and ACK records, handler logs, kernel server-death receipts, binary hashes,
source hash, actual exits and timing. Existing worker-only and graceful-restart
proofs are preserved against their published fixture bytes; this helper now
shares the workflow assertions through adapters for library/process clusters.

These are abrupt **process deaths on a live host**. The host's kernel/page cache
is not destroyed, so VM/power/storage-loss faults remain unqualified. The actual
100,000-entry graph boundary, broader partition/security/deployment/native
matrices, complete current/race/extended simulation, public continuation
admission, production collection and all remaining original requirements stay
open. The current normal and older151 race campaigns remain live on frozen cuts.
