# Common-clock worker scheduling

`worker.WithTimerClock(domain, bounds)` installs SDK domain support on production
and modeled workers. Bounds calls have3s contexts and reject empty, reversed or
canceled replies. Domain operations record both bounds separately from legacy
server-clock lookups. All timer schedules retain the worker's lease-renewal
boundary, transport acknowledgment classification and new-schedule counters.

Native publication separates canonical FireAt from a translated ScheduleAt hint.
The hint is the current scheduling leader time plus the remaining duration from
the canonical lower bound. Leadership may race the lookup or change while the
schedule is retained; a native delivery is never due proof for a tagged timer.
The emitted message carries canonical deadline/domain and invocation/step.
Fallback payloads retain the canonical deadline/domain without a physical clock
lookup. Identity and duplicate-publication behavior remain stable.

Full worker race suite passes31.204s. Controls verify±60s hint translation,
canonical fallback payloads, duplicate counters, failed bounds and canceled or
unknown-domain calls without publication, and lost acknowledgment classification.
Two production-worker deterministic workloads pass58.331s:1,000legacy fresh-
wakeup schedules and1,000common-domain schedules. The new scenario covers all16
API×±60s delivery-skew×duration combinations, eight genuine waits and nine handler
replays, exact replay for ten seeds, a single immutable42result, journal integrity
and physical modeled consumer drain. Its new workload is explicitly not yet in
the pinned full-release corpus or100,000-seed gate.

The native R3 header contract passes3.53s (package4.563s): actual scheduled
WF_RUN targets retain domain/deadline/generation/step, even when the canonical
deadline differs from the hint by60s. This is protocol preservation evidence,
not a workflow due decision or a server-clock transition test.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./worker -count=1 -timeout=3m
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run 'Test(WorkerCommonClockAcrossSkewedDeliveryTimestamps|SeededWorkerFreshTimerWakeupReplay)$' -count=1 -timeout=3m -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./integration -run '^TestNativeTimerDeliveryPreservesDomainDeadline$' -count=1 -timeout=2m -v
```

The option is not enabled in CLI/default deployments or the R5 fault fixture.
All readers and repairers must support the domain before tagged writers run.
Probe provisioning, trusted topology configuration, common-clock repair-loop
wiring, migration, pinned clock-transition/repair models and native admitted-cut
recovery remain required. The ahead-clock60.27s failure and incomplete sustained
behind-clock admission remain open. Full logs and source/evidence hashes are
retained; no release gate is cleared by this individual implementation slice.
