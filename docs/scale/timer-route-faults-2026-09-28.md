# Timer route-fault audit

The opt-in `WF_TIMER_SLEEP_CHAOS=1` test extends the 10,000-sleep workload in
[`timer-sleep-2026-09-28.md`](timer-sleep-2026-09-28.md). It starts with every
timer scheduled and at least half still active. A three-node route mesh then
isolates nodes 2, 1, and 2 for eight seconds each, verifying route counts on
every cut and heal. Two seconds into a cut, a pre-created worker on the
majority node begins pulling the isolated owners' partitions. The durable
consumers use a 10-second `AckWait`, two-second progress heartbeat, and 32
concurrent messages per partition. A timer reconciler scans from the majority.

This test is a **failing release gate**. In a full 10,000-invocation run, all
10,000 timers were scheduled and 9,387 remained active before the first cut.
The completed handler count stalled at 9,735. It was still unchanged more
than five minutes after the latest possible `fire_at`, so the diagnostic run
was stopped. That run preceded the final bounded-read changes. A later
100-invocation run with 1–10-second sleeps completed every workflow but had
48.1 seconds p99 handler lateness, above the plan's 30-second chaos target.
A 100-invocation rerun completed all workflows with zero early or stuck
completions and p99 37.18 seconds.

On the expanded 4-CPU, 15-GiB VM, a fresh full run scheduled all 10,000 timers
with 9,314 still active before the first cut. All 10,000 handlers completed,
all results and terminal journals were read, and `WF_RUN` drained in 77.0
seconds after starts began. No handler finished early or five minutes late.
The route sequence lasted 24.8 seconds. Handler lateness was 2.98 seconds at
p50, **31.85 seconds at p99**, and 45.61 seconds maximum, so the 30-second
p99 gate still failed. Workers recorded 9,359 redeliveries, 144 fencing
events, and 57.42 seconds maximum enqueue-to-lease latency. This run proves
liveness for this fault schedule but does not prove the latency target or
consistent liveness across seeds and failure modes.

Two instrumented full reruns measured lease contention separately from other
acquisition failures. With the usual one-second retry after `ErrHeld`, all
10,000 completed at p99 44.48 seconds; workers recorded 19,875 redeliveries,
13,634 lease contentions, and 313 lease acquisition failures. A trial
two-second retry brought one run to p99 27.87 seconds but its timer reconciler
exited after a lease initialization timeout. The reconciler now retries that
wrapped lease-loss error. A second two-second run then completed all 10,000
at p99 43.43 seconds, with 20,975 redeliveries, 15,424 contentions, and
2,220 acquisition failures. The longer retry was reverted because it did not
consistently meet the gate. These runs show that both held leases and failed
acquisitions contribute to the recovery backlog; they do not isolate a single
server-side cause of the timing variance.

Lease acquisition now uses revision CAS to reclaim an uninitialized key that
has remained for at least one second after its creator failed before writing
the fencing epoch. A fresh key remains held. Workers also make bounded,
revision-checked cleanup attempts after an uncertain release; cleanup refuses
to delete a successor's epoch. Real KV tests cover both cases, and the
45-second worker-partition fencing test still passes. Three full route runs
with uninitialized-key reclaim finished every invocation at p99 24.89, 20.52,
and 40.21 seconds. After adding uncertain-release cleanup, two further full
runs finished at p99 42.35 and 38.89 seconds. The 30-second target remains
variable and unmet as a reliable gate. A full no-fault 10,000-timer run after
both lease changes passed at p99 728.8 ms with zero redeliveries and fences.

Per-worker and per-original-owner-node diagnostics show the tail is spread
across the cluster. A run before consumer retry backoff reached p99 61.97
seconds: 2,210 timers exceeded 30 seconds across all three owner-node groups,
with 34,315 redeliveries, 23,848 held-lease retries, 3,897 lease acquisition
failures, and 2,135 fencing events. Partition loops now back off from 100 ms
to at most two seconds after repeated transient consumer errors and reset on a
healthy fetch. Two full runs with that change completed all workflows at p99
31.32 and 37.16 seconds; the 30-second gate still failed. A trial that paused
all of a worker's partition loops after lease API errors reduced such errors
but shifted load to held-lease retries and finished at p99 44.53 seconds. A
delivery-count-based delay for held leases reduced redeliveries to 6,740 but
finished at p99 39.54 seconds. Both trials were reverted. The evidence points
to a cluster-wide recovery backlog, not just delayed work on the isolated
node, and leaves the p99 target open.

The no-fault 10,000-timer run after the consumer backoff change passed at p99
1.66 seconds, under its two-second gate. A focused consumer-leader test was
also running during its early setup, so this is a correctness check rather
than an isolated throughput baseline.

Further 100-invocation diagnostics showed that wakeups can wait tens of
seconds between enqueue and lease acquisition. With the reconciler disabled,
p99 was 36.7 seconds, the longest enqueue-to-lease wait was 32.7 seconds,
and workers saw 350 redeliveries. Shortening `AckWait` from 10 to 3 seconds
worsened p99 to 46.8 seconds, so the 10-second setting remains. With the
reconciler enabled, another run measured 54.1 seconds p99, 48.7 seconds
maximum enqueue-to-lease wait, and 2,567 redeliveries. These are separate
runs and show variability; the 30-second target remains unmet. The test now
records fault duration, redeliveries, fencing, and maximum enqueue-to-lease
latency. `WF_TIMER_SLEEP_SKIP_RECONCILER=1` isolates dispatch for diagnostics.

The fault runs exposed several recovery gaps that now have bounded retries:
the timer reconciler no longer exits on a transient stream response or a
cursor CAS conflict; `Client.Await` retries bounded reads; worker timer
publication, server-time lookup, journal read, and invocation/signal lookups
use bounded attempts; empty pull loops refresh consumers when messages remain
unhandled. The test journal reader now also retries transient NATS
`ErrNoResponders` during route isolation; a smaller run previously failed
its result audit when that response reached the reader. These changes
preserved the no-fault 10,000-sleep result at
826 ms p99 in a subsequent full run. After the final consumer-refresh
adjustment, another full no-fault run completed all 10,000 invocations with
775.9 ms p99, 1.04 seconds maximum, and zero early or stuck completions.
At that point the route-fault gate had not passed consistently; the broader
chaos matrix remained open.

On 2026-09-29 the route-fault gate was clarified to measure recovery from
the later of each timer's `fire_at` and the final confirmed route heal.
Completions during the fault have zero post-heal delay. The test still logs
raw `fire_at`-to-completion lateness and still rejects early completions and
completions more than five minutes after `fire_at`. A 100-timer diagnostic
passed with raw p99 27.10 seconds and post-heal p99 8.05 seconds. A full
10,000-timer run passed with raw p99 35.10 seconds, post-heal p99 13.04
seconds, post-heal maximum 14.66 seconds, all results and terminal journals
verified, and zero early or stuck completions. The route sequence lasted
24.83 seconds. Further full runs are needed to establish consistency under
the clarified gate; the raw 30-second target remains unmet in that run.

A second full 10,000-timer route run passed with raw p99 36.60 seconds,
post-heal p99 18.36 seconds, post-heal maximum 22.89 seconds, and zero early
or stuck completions. All results and terminal journals were verified; the
route sequence lasted 24.90 seconds. A 1,000-timer repeat also passed with
post-heal p99 28.42 seconds. An earlier 1,000-timer run failed during result
auditing after 970 handlers had completed; the inspected invocation already
had a completed journal and result, so its cause remains unconfirmed. The
full-scale recovery gate has now passed twice, while that smaller audit
failure still warrants investigation.

The result audit now retries a readable but nonterminal journal for at most
45 seconds after `Await` reports a terminal result, then reports the last
entry kind and transport error. A subsequent 1,000-timer, 1–10-second sleep
run completed all results and terminal journals, but missed the post-heal
latency gate at p99 30.98 seconds. It recorded 22,521 redeliveries and
21,611 lease contentions. This shorter, more synchronized diagnostic remains
open; its sleep distribution differs from the specified 10,000-timer,
1–60-second proof.

After the terminal-journal audit change, a third full 10,000-timer run passed
with raw p99 31.23 seconds, post-heal p99 16.90 seconds, post-heal maximum
19.00 seconds, and zero early or stuck completions. All results and terminal
journals were audited. This is three full passes of the clarified route gate;
the separate 1–10-second diagnostic still has the recorded p99 miss.

A fresh 1,000-timer, 1–10-second diagnostic on 2026-09-29 passed with raw p99
35.00 seconds, post-heal p99 13.62 seconds, post-heal maximum 14.02 seconds,
and zero early or stuck completions. All results and terminal journals were
audited, `WF_RUN` drained, and the route sequence lasted 24.90 seconds.
Workers recorded 2,483 redeliveries, 1,674 lease contentions, 295 lease
acquisition failures, and 84 fencing events. The earlier 30.98-second
post-heal miss with much higher contention remains unexplained; one passing
rerun does not establish consistency for this synchronized variant.

Three more consecutive local repeats of the same 1,000-timer diagnostic
passed on 2026-09-29. Raw p99 values were 38.27, 27.82, and 35.91 seconds;
post-heal p99 values were 14.98, 11.62, and 12.25 seconds. Each run
completed all 1,000 timers with zero early or stuck completions and audited
their results and terminal journals. Redeliveries ranged from 926 to 4,175;
the third run had one post-heal outlier at 30.86 seconds while its p99 stayed
at 12.25 seconds. The prior 30.98-second p99 miss remains a real outlier and
its cause is not established by these passes.

To reproduce the smaller diagnostic run:

```sh
WF_TIMER_SLEEP_CHAOS=1 WF_TIMER_SLEEP_COUNT=1000 \
  WF_TIMER_SLEEP_MAX_SECONDS=10 WF_TIMER_SLEEP_TIMEOUT_SECONDS=600 \
  go test ./integration -run '^TestTenThousandRandomSleepsDuringRouteFaults$' \
  -count=1 -timeout=12m -v
```
