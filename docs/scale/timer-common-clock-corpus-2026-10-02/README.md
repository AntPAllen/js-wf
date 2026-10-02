# Common timer clock enters the deterministic release corpus

The production-worker common-clock workload now honors `SIM_SEEDS`, uses the
`TestSeeded` naming convention and preserves failures through `FAULT_TRACE_OUT`.
Generic trace replay recognizes `worker_common_clock_timers`. Sixteen new pins
cover all four APIs × two delivery timestamp skews × two durations; existing
legacy traces retain their original workload and semantics. The corpus now has
200 pinned traces, but this turn verifies the 16 new ones, not all 200.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 SIM_WRITE_COMMON_CLOCK_PINS=1 go test -race -p=1 ./sim -run '^TestSeededWorkerCommonClockAcrossSkewedDeliveryTimestamps$' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./sim -run '^TestPinnedRegressionCorpus/worker-common-clock-' -count=1 -v
```

The 1,000 generated schedules pass in 29.34s / package 30.361s; the first ten
replay exactly. All 16 saved traces load and replay exactly through the generic
corpus in package 1.548s. Sources, pins and complete logs are hashed in manifest.

The workload exercises actual worker/SDK/journal decisions with an ideal common
clock provider and ±60s scheduling/delivery timestamps. It does not model the
independent sampler, correlated clock faults, leader transitions or repair
provider outages. These require additional models and native admitted-cut gates.
Registration enables inclusion in future comprehensive 100k runs; it does not
retroactively expand the scope of already running older-source campaigns.
