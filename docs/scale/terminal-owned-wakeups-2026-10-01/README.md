# Durable terminal duplicate probe after owned delivery

The 500-child consumer-leader fault exposed live duplicate parent wakeups after
terminal completion: the baseline hosted raw test failed in 59.84 seconds with
373 queued records despite initially successful job metadata. Its parent made
129 full history reads totaling 14.883 seconds. The corrected pipeline run
36850691761 at 0c564d6 reports failure as expected; metadata is retained here.
The separate baseline full-restart test genuinely passed. These observations
remain distinct from the plan's five-minute completion and per-invocation p99
requirements; the 30-second drain check is a fixture diagnostic.

## Runtime change

Each worker keeps at most 1,024 local terminal hints in a synchronized FIFO.
Hints only select a cheaper durable probe; they cannot authorize an ACK.
A successful owned duplicate still acquires/releases its normal lease, reads
canonical outcome and current invocation generation, requires matching positive
InvSeq, rejects tombstones/corruption, and completes generation-scoped parent
notification. It then skips complete history replay and repeated MaybeSnapshot.
Uncertain or invalid probes fall back to ordinary execution/terminal repair.
Scheduled canceled-timer deliveries retain their history lookup and metric.
Eviction or worker restart restores ordinary replay; no hint is durable state.
Healthy nonterminal deliveries receive no additional probe without a hint.

Terminal persistence plus parent notification records a hint. For ordinary
handlers with a snapshot transport, the hint remains disabled until the
following automatic snapshot operation succeeds. The first prototype skipped
snapshot repair after an uncertain write: the existing snapshot worker workload
caught this at seed 2 with 262 live entries instead of 16. Its failure is retained.
The final implementation passes the unchanged snapshot repair assertions.
Continuation handlers own their checkpoint publication and do not run this
ordinary automatic snapshot operation.

## Native confirmation

The final race-instrumented 500-child consumer fault passes in 42.30 seconds
(43.328 seconds package time). It verifies physical SIGKILL, successor consumer
leader while the old node is dead, replica catch-up, every child queue identity,
retained parent prefix, all 501 immutable outcomes and 4,506 journal entries.
Child raw-start p99 is 20.410 seconds, maximum 21.764 seconds; parent delay from
last child completion is 3.902 seconds. No heal-time adjustment is used.
WF_RUN physically drains to zero; all 64 consumers have zero pending/ack-pending.
The parent makes four full reads totaling 1.936 seconds and 497 canonical terminal
probes. Every journal append still renews its lease unconditionally. Raw log,
operation JSON and all three final server reports are retained. A preliminary
prototype native pass is not substituted for this final-source result.

Existing continuation canceled-timer/version replay, ordinary canceled timer
and held-lease terminal fixtures pass together under race in 33.024 seconds.
The continuation retains one canceled-timer no-op and performs no archive read;
held terminal wakeups preserve the healthy owner's revision and single effect.

## Deterministic proof and controls

The new worker_terminal_owned workload passes 100,000 seeds in 83.498 seconds:
35,001,268 transport events, all eleven modes and maximum virtual time 5.1 seconds.
Each seed processes 32 duplicates through production worker, lease, journal,
outcome and notification decisions. It verifies unchanged journal/outcome,
one handler/effect (or a Failed outcome), physical drain and no remaining lease.
Modes include completed/failed, lost state read, mismatched generation,
tombstone, malformed or missing state, normal/dropped/lost-ack parent
notification, and lost ACK. Invalid durable data cannot ACK on a hint; transient
or missing state repairs through the original path. First ten schedules replay
exactly and two separate processes produce identical traces. Eleven new pins
retain each mode. A fixed 500-duplicate workload covers/replays all eleven modes.

The modeled read wrapper charges 100ms for each post-terminal complete history
scan. It models observed client cost, not a NATS server cause or the separate
snapshot interval. Three compiled production overlays fail semantically:

- Disabling hints: 32 duplicates incur 3,200ms, and 500 incur 50,000ms; tests fail
  on repeated-read cost in 0.104 seconds, not a wall-clock timeout.
- Trusting the hint before durable reads: stale-generation seed 2 ACKs one
  forbidden delivery (31 pending instead of 32), failing in 0.008 seconds.
- Enabling hints before automatic snapshot success: seed 2 leaves 262 live
  entries instead of 16, failing in 0.031 seconds.

Overlay sources and failure logs are retained; none is promoted. A bounded-cache
unit check also verifies pending snapshot readiness, replacement, FIFO eviction
and that repeated resident hints cannot evict other entries. Full sim/worker/
journal suites pass (131.156/36.168/0.006 seconds); vet passes. The 132-pin corpus,
new owned workload and fixed 500 case pass under race in 44.801 seconds.
Nine existing traces affected by skipped terminal reads were regenerated from
the same seeds/initial fault choices. Previous traces are retained compressed;
the held-terminal, worker/compactor and snapshot-repair pins stay unchanged.
Replay continues to reject differing recorded choices/events. Source hashes
and the exact regenerated-pin list are retained.

This closes the observed terminal full-replay backlog slice with local evidence.
Latest-source comprehensive 100,000-seed simulation, the
whole-matrix 200-seed campaign, five-node 24-hour soak, remaining combined faults,
million-timer final audit and other capacity/operational gates remain open.
The ongoing prior-source full campaigns are not restarted or counted as proof
of this runtime revision. This one native cut does not clear the full 500-child
fault matrix, and the deterministic cost model is not server-cause attribution.

## Hosted confirmation

Run 36852513175 at ca82e23dc67b36dfb47020f371e5f36f33724317 now completes.
Both actual raw test logs, not only job status, PASS at the same source. Consumer
leader recovery passes in 32.85 seconds with all 501 terminals and 4,504 entries;
raw-start child p99 is 16.809 seconds and parent last-child delay is 2.608 seconds.
WF_RUN and all 64 consumer pending/ack-pending counts are zero. The parent has
two full history reads totaling 952.418ms and 499 canonical terminal probes.
Full restart passes in 22.20 seconds with the same retained-state counts.
Raw complete logs and job metadata are retained as hosted-shortcut-* here.
The earlier failed baseline remains independent evidence. This confirms these
two focused cuts; comprehensive simulation, the full 500-child matrix and all
other unsatisfied release requirements remain open.
