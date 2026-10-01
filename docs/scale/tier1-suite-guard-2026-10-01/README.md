# Tier 1 complete-suite evidence guard

The local full simulator package passed in 107.158 seconds at runtime source `98b5030c81058fbf6f80fd1b667512446d3d8cff`, configured for 1,000 seeds per seeded workload. The reviewer and workflow were uncommitted changes under validation; production and simulator source were unchanged. The original Go JSON stream, compiled test inventory, regression file inventory, source and reviewer report are retained here.

The guard verified 147 top-level passes, the two documented trace-only skips, and all 168 pinned regression subtests. It requires each listed test to execute and finish once, rejects failed or skipped workload/subtests, and checks package completion and aggregate counter consistency. Nine focused controls cover missing/duplicate tests, failures, skips, package identity, missing pinned regressions and corrupted coverage. All 61 Python checks passed; workflow YAML and diff checks passed.

Reproduce the retained review:

```sh
gzip -dc docs/scale/tier1-suite-guard-2026-10-01/events.jsonl.gz > /tmp/tier1-guard-events.jsonl
python3 scripts/check-tier1-suite.py --events /tmp/tier1-guard-events.jsonl --inventory docs/scale/tier1-suite-guard-2026-10-01/inventory.txt --regressions docs/scale/tier1-suite-guard-2026-10-01/regressions.txt --source docs/scale/tier1-suite-guard-2026-10-01/source.txt --seeds 1000 --output /tmp/tier1-guard-result.json
```

The extended workflow now retains structured events, source/inventories, timings and the checked result. This is a 1,000-seed validation of the reviewer, not the 100,000-seed release gate. Aggregate telemetry reports seed configuration, not independent proof that each workload exercised every seed. Current campaign 36929826425 was launched before this workflow change and must be reviewed from its original logs; it must not be restarted merely to apply the reviewer.
