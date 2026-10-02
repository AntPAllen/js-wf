# Domain-aware timer repair decisions

TimerScan, SuspendedScan and FallbackTimerScan retain and interpret clock_domain
from deadline requests, timer-select cases and fallback records. Their optional
DomainNow adapter must provide a conservative lower bound in that exact domain.
There is no fallback to worker wall time or one stream leader's clock for tagged
records. Missing/failed/empty bounds fail closed. Calls carry a3s maximum context;
already canceled requests are rejected, as are canceled replies. Suspended waits
retain the configured grace. Multi-case repair can still use another independently
ready case when one clock or signal read fails.

Fallback repair reads its legacy server clock lazily, so a tagged-only scan does
not depend on legacy-clock availability. It retains not-yet-due timers and only
deletes after an acknowledged wakeup (except retired-generation cleanup). Timer
and fallback repair events include the deadline domain. Existing untagged
records use their original clock and stable publish/delete rules.

Full repair race suite passes12.964s. After the cancellation-boundary addition,
final focused domain/cancellation controls pass1.018s:20timer/fallback cases,
four suspended wait kinds with grace, and a canceled context. Tests refuse any
legacy-clock read for tagged deadlines, reject failed clocks before writes,
retain future fallback records and check acknowledged domain evidence. Existing
native duplicate-publish/retirement and multi-select contracts run in the suite.

Six existing seeded production-repair/fallback workloads pass1,000seeds each,
plus cooperative scanners and partial-read controls, in50.791s. These regressions
preserve legacy models; they do not prove common-domain runtime recovery.
Commands:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./reconcile -count=1 -timeout=3m
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./reconcile -run 'Test(RepairScannersRespectDomainClockAndFailClosed|SuspendedDomainTimersIncludeGrace|DomainRepairClockHonorsCanceledContext)$' -count=1 -timeout=2m -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./sim -run '^Test(Seeded(TimerScanReplay|SuspendedScanReplay|SuspendedLoopReplay|FallbackTimerPipelineReplay|WorkerFallbackTimerExecutionReplay|WorkerFallbackTimerLoopExecutionReplay)|SuspendedScanKeepsVerifiedWakeupsOnTransientRead|CooperativeSuspendedScannersReplay)$' -count=1 -timeout=3m -v
```

No production worker emits tagged records or installs these clock adapters yet.
Probe provisioning, worker/CLI wiring, native/fallback domain-preserving scheduling,
migration and new end-to-end seeded/native clock gates remain required. The real
ahead-clock progress failure and incomplete sustained behind-clock campaign remain
open. Complete logs and source/evidence hashes are retained beside this file.
