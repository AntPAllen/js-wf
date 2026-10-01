# Timer repair publication observers

Production TimerScan and FallbackTimerScan expose optional synchronous repair
observers. The unchanged enqueue decisions report actual publication outcomes:
dry_run, uncertain (including committed/lost acknowledgement), or acknowledged.
Due journal timers identify timer/timer_start/timer_await, retained invocation
and pending journal sequence, and fire time. Fallback timers identify retained
timer sequence, invocation generation, fire time and step, including step zero.
The publication is observed before deleting the timer. Deletion failure cannot
turn an acknowledged wakeup into an uncertain publication. Retired/future or
otherwise ineligible records emit no enqueue event. RunRepairLoopObserved now
accepts timer and fallback-timer through the existing fenced cursor loop.

The existing production-decision timer and fallback pipeline workloads each
pass100,000 seeds (200,000 total) in38.413s package:15.98s fallback,22.42s timer.
They check exact event/decision counts, retained source fields, due times,
lost or dropped acknowledgements, deduplicated retry and deletion faults.
The first ten schedules replay exactly and seed42 compares across processes.
Transport traces and workload IDs are unchanged; observer evidence is not used
to make scheduling choices. Focused race checks, including all168 pinned
regressions, pass11.263s sim package; real R1 fallback duplicate publication
passes0.10s test/1.117s package and logs its actual serialized observer event.
Dry-run preservation and zero-step observations are checked separately.
All40 Python tests and reconcile/sim/integration vet pass.

An overlay that records an uncertain publication as acknowledged fails both
production scanner workloads at seed1 in0.005s package, with original failure
traces retained. The first control exposed a timer test assertion being returned
as an error that the existing fault case could mistake for expected transport
failure. A separate sticky evidence gate now propagates that assertion after
the modeled operation without changing transport behavior. The initial control
is retained visibly; only the final two rejecting controls count as proof.

This establishes these observer boundaries through seeded in-memory transports
and one real duplicate acknowledgement. It does not establish a full final-source
100k suite, arbitrary server causes, full fallback fault deployment or the24-hour
matrix. Existing R5 runner campaigns only install the repair observers for the
scanners they actually launch; adding these APIs does not invent timer events
in those earlier campaigns.
