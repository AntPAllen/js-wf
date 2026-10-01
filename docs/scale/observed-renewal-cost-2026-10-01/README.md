# Projected renewal costs from real mixed seed 4

The input fixture extracts the exact 52 KV Update nanosecond durations and
journal kinds/indices for mixedsignal/mixed-00-0 in hosted fcf4537 run
36835269534. It also records the final four signals arriving 317.721021 ms into
the first costly renewal (derived from its update duration and logged time
since last enabling event). Scheduler granularity is milliseconds: each renewal
has floor/ceil profiles and the arrival boundary rounds upward to 318 ms.
Original nanoseconds and the fixture hash remain in the retained source/trace.

Actual production workers run three dispatches: initial empty signal wait,
a drain of twelve buffered signals (with four arriving inside the first
renewal), and replacement completion with those four. KV initialization updates
are separate and have zero injected cost. Every production pre-append renewal
must match the source's index/kind and rounded duration. The intermediate cut
has 40 records, and the final retained-state audit has 52 contiguous records,
16 consumed signals, immutable result 120 and a drained modeled run queue.
Real journal/signal reads, appends, lease acquisition, heartbeat contention,
release, Raft, physical persistence and process cuts are not timed in this model.

Floor/ceil project 30.155/30.204 seconds from the last enabling event using
renewal costs alone, exceeding the original 30-second gate. The zero-cost
control finishes at zero virtual time. Floor/ceil are explicit expected failing
liveness controls, never counted as clean release seeds. This is a projection
of observed costs through matching SDK/worker journal decisions, not a full
replay of the real failure or proof of its server-side cause.

## Verification

100,000 checked schedules pass their protocol/cost assertions in 132.853 seconds:
33,334 ceil, 33,413 floor and 33,253 zero profiles. There are 36,300,000 transport
events and maximum virtual time 30,526 ms. This repeats three fixed profiles;
it does not establish 100,000 clean fault schedules. First-ten exact replay,
separate-process seed-42 byte identity, pinned zero/floor/ceil traces and race
pass. Workload plus corpus race passes in 26.748 seconds; the finalized three-pin
corpus race passes in 2.891 seconds. Vet passes. Existing pins are unchanged.
A compiled cached-pre-append-renewal mutation fails semantically with zero of
52 required renewals (0.006 seconds); the mutation is not promoted to runtime.

The real disk attribution maps node 0's 484 lease-Raft writes to 34.076336
seconds and 259 lease-data writes to 18.236598 seconds of delayed syscall
windows. Consumer/journal writes overlap those windows. These whole-trace sums
are not the invocation's critical path or RPC server execution. They do not
identify each RPC's leader or explain why its KV Update was slow. Raw artifacts are in
the sibling mixed-seed4-fcf4537 proof; the attribution output is retained here.

The hosted 1bcf5ef eight-cut process restart job passes under race in 140.407
seconds, and its chaos-smoke job also passes, validating the earlier bounded
startup-election fixture fix. The complete standard workflow at 1bcf5ef also passes; its full log and job
conclusions are retained. No liveness gate, mandatory
renewal, fencing or production adapter is changed. Latest-source full Tier 1,
200 clean whole-matrix seeds, 24-hour five-node soak and other plan gates remain
open. Original million timers continue on their original process/stores.
