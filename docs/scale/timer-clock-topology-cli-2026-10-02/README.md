# Independent clock topology and worker CLI evidence — 2026-10-02

The opt-in CLI now uses one shared monotonic clock for SDK timers and elected
repair loops. Trusted topology validates independent physical sources; bootstrap
creates exact tagged R1 memory probes and rejects conflicting configurations.
Defaults and the R5 fault fixture remain legacy.

All commands used `GOMEMLIMIT=512MiB GOMAXPROCS=2` and race instrumentation:

```sh
go test -race -p=1 ./runtimeclock -count=1 -v
go test -race -p=1 ./integration -run '^TestRuntimeClockProvisionsExactIndependentProbesAndSurvivesLoss$' -count=1 -v
go test -race -p=1 ./cmd/wf-worker ./reconcile -count=1
go test -race -p=1 ./cmd/wf-worker -run '^TestWorkerRunner(RejectsClockWithoutRepairsOrConfig|UsesIndependentClockForNativeAndFallbackTimers)$' -count=1 -v
```

| Evidence | Result |
| --- | --- |
| topology-race.log | PASS 1.121s: topology/parser, cache expiry and cancellation |
| native-provision-race.log | PASS 3.84s / package 4.867s: exact probes, unchanged conflict, fresh agreement after one node loss; uncertainty 123.837317ms |
| cli-reconcile-race.log | PASS CLI 32.501s / reconcile 13.186s; before adding the new CLI tests, same production code |
| cli-native-fallback-race.log | PASS 25.83s / package 26.922s: actual CLI/plugin completes a tagged timer on each backend, fallback record drains, invalid configuration rejected |

The last test's native 15.56s and fallback 9.03s include provisioning, startup and
shutdown; they are not timer latency measurements. Each backend runs one healthy
timer. Readiness polls the same runner with bounded metadata reads. A development
readiness timeout was fixed in the test; product recovery targets were unchanged.

`manifest.json` records source and complete log SHA-256 hashes. This evidence does
not prove clock-skew, leader-transition, restart, full corpus or release acceptance.
The ahead 60.27s failure and incomplete behind sustained gate remain open.
