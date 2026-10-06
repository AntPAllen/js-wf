# Real workflow shared latency reducer oracle

Fresh current NATS2.15 R3 in-process race run at `14efcfb` passes in **47.89 s**.
All 160 completed short/timer/signal/parent/child workflows have identical samples
under the independent frozen pre-extraction point implementation, shared reducer,
serial, parallel and cached metadata point adapters. Actual terminal data still
rejects an impossible post-heal deadline in serial/parallel/cached adapters.
The pre-extraction function body is independently retained at source `36f82cf`;
its renamed body hash is recorded in the reducer preparation.

Actual SDK PID 2463458 is closed, SHA256
`2a8884ffdd0217e2c68d39ea39b5b9c1071c5ea018c886284220de1c58dfa73b`.
All699 selected Git Go/module source files match captured before/after inventories;
actual executable build metadata confirms race, clean source and pinned NATS2.15 /
client1.54. Library servers are embedded in that SDK; there is no separate server
process claim. External compiler inputs were not exhaustively captured. Profile:
GOMAXPROCS2 / GOGC500 / GOMEMLIMIT2GiB; long soak and million candidate overlap.

Full archive **2,803 members / 32,153,215 bytes / two parts** read back completely.
This qualifies the reducer refactor's actual same-fixture sample equivalence;
it does not qualify bulk acquisition, large cohort throughput, fault recovery,
snapshot/purge combinations, final-source matrices or24h gates. Production runtime
and original audit/latency deadlines remain unchanged. Bulk implementation follows.
