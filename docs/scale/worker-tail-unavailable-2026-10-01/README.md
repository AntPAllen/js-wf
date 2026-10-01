# Production worker recovery from unavailable journal tail reads

The 463f648 hosted pressure failure records three journal tail lookups returning
API 503/10008 (JetStream temporarily unavailable) in the first delayed baseline
row. Existing cooperative journal tests cover generic transport loss; this new
workload exercises the explicit observed API edge through actual production
workers. It does not simulate the server mechanism that produced the error.

The seeded workload selects Started, StepRequested, StepCompleted or Completed
append, and one/two/three consecutive unavailable reads. Each failed delivery
stops at its actual NAK, requires one retained pending run, byte-identical raw
journal contents from the failure cut, no lease and no terminal outcome. A
fresh worker handles each attempt. The successful successor must drain the run,
preserve a four-entry CAS/fenced journal, satisfy the shared raw-state invariant
checker and return exact result 42.

When the StepCompleted tail read fails after an effect ran, the outcome was not
recorded; the effect is permitted to run again (failures+1 calls). Other failure
stages run it once. This preserves the existing at-least-once external effect
contract and exactly one retained outcome; it is not an exactly-once effect claim.

100,000 schedules pass in 17.888 seconds: 200,000 scheduler choices, 6,254,317
transport events and maximum virtual time 3,000 ms. All twelve combinations
are covered. First ten exact replays and separate-process seed-42 trace identity
pass. Twelve pinned traces cover all combinations. The finalized workload plus
full pinned regression corpus pass under race in 8.009 seconds; vet passes.

A compiled production worker overlay ACKs execution retries instead of NAKing.
It fails semantically in 0.004 seconds on pending=0 versus the required retained
run, preserving the failed journal prefix. Its initial unused-delay build error
is retained and excluded as mutation evidence. An initial fixture treated the
persisted outcome's base64 byte representation as raw JSON; that fixture error
is retained and corrected to decode bytes. Neither control is promoted.

This is a native Tier 1 reproduction of the client-visible recovery decision.
It does not establish server cause, leader stability, actual disk timing, the
pressure throughput gate, the whole fault matrix or latest full-suite release
proof. Existing full simulation runs predate this new workload. No production
runtime policy, timeout, TTL, unconditional renewal or p99 target changes.
