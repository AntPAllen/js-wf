# Durable SDK timer clock domains

Full SDK race suite passes13.318s. Five focused production-worker simulator
workloads (1,000seeds each plus existing replay/pin checks) pass70.148s:
fresh wakeups, timer clock transitions, native timer execution and both retained
fallback execution paths. These simulator regressions preserve legacy behavior;
the clock-transition workload remains a characterization of its existing miss.
Commands:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./wf -count=1 -timeout=2m
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^TestSeededWorker(FreshTimerWakeupReplay|TimerClockTransitionReplay|TimerExecutionReplay|FallbackTimerExecutionReplay|FallbackTimerLoopExecutionReplay)$' -count=1 -timeout=3m -v
```

`TimerClockSupport` explicitly opts a workflow context into a durable clock
domain. New positive timers record `clock_domain` beside their deadline. The
origin uses the interval's upper bound and due decisions use its lower bound.
Scheduling receives the domain instead of silently using the legacy transport.
Sleep, Await, SelectSignal and generic Select retain domain metadata in their
requests/cases. Timer observations use the due clock, not a shifted delivery.

Eight API×delivery-skew cases cover±60s, upper-bound origins, intervals straddling
due, missing and mismatched support, no journal change on those pending failures,
canonical completion, and completed replay without clocks. Invalid/zero/reversed
or unavailable bounds cannot create a request. A legacy request remains in its
original domain after new clock support is installed. All existing SDK tests run,
including immediate waits, signal priority, cancellation and replay contracts.

No production worker installs this support yet. All workers and repairers must
be upgraded before tagged writers can be enabled: older SDKs ignore unknown
JSON fields and cannot safely consume tagged deadlines. Clock probe provisioning,
worker wiring, native schedule translation, canonical repair, live migration and
end-to-end deterministic/native recovery evidence remain required. Existing
untagged records are not silently reinterpreted. The real ahead-clock60.27s
progress miss remains open; these tests do not clear that or any release gate.

Complete terminal logs and source/evidence hashes are retained beside this file.
