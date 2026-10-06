# Corrected partition200 generator launch

Original run37486948256 failed before workload execution because its generator sparse checkout omitted the integration source required by a registry test. A fresh exact-source checkout reproduces that failure; adding `integration` passes all24 matrix tests plus the workload-source test.

Corrected run37500390198 executes source `d9093d74f4e18562d70173c0fc6715c394a0314a`, rowpartition/seeds200/start1/duration10m. Workload deadlines, fault count/route durations, retained integrity, all raw history models, latency and physical drain are unchanged. Dispatch is a new corrected generator source, not a restart on observer expiry. The old terminal failed run and complete terminal collection remain preserved and unqualified.

A persistent read-only observer follows this exact run ID/source and retains provider terminal logs/artifacts. Observer expiry is not native failure. Neither queued launch nor observer output qualifies the200-seed row; exact source,201 successful jobs,400 artifacts and each actual10m shard's raw required evidence must pass the independent full-row collector/reviewer.
