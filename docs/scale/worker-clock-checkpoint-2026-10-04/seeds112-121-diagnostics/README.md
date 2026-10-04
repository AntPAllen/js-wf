# Instrumented diagnostics for failed clock seeds 112 and 121

The original failed shards are preserved in [105–130 failure evidence](../failed-shards-105-130/). Both failures exhaust the 60-second batch-90 retained audit, but old source does not record individual attempt timing. Two focused ten-minute runs are dispatched at exact `3b2999ec9ec3a9dd131ffe742fdfd48c3cfbaf34` using the existing worker-clock profile and its current per-attempt timing/primary-error instrumentation:

| Seed | Run | Workload job | Observed state |
| ---: | --- | --- | --- |
| 112 | [37183948633](https://github.com/AntPAllen/js-wf/actions/runs/37183948633) | 111381924505 | independently accepted; see completed proof |
| 121 | [37183950090](https://github.com/AntPAllen/js-wf/actions/runs/37183950090) | 111381922136 | independently accepted; see completed proof |

`dispatches.json` preserves exact commands, resolved source, timestamps and accepted run URLs. Run/job API snapshots independently confirm the exact source and live workload jobs. `runtime-equivalence.json` checks all 685 selected runtime/simulation/Tier1 producer bytes against the accepted race source `283ba32`; integration code differs from historical `79915ca`, so these are current-source diagnostics rather than a replay of an identical historical binary.

Both use the unchanged three 20-second audit attempts inside the 60-second total limit, ten-minute workload, five actual worker subprocesses and original clock fault cadence. Current instrumentation records each attempt's start/end/deadline/partial report/error and preserves the primary failure before cancellation. No production fix, timeout relaxation or original server-cause attribution is claimed. A dispatch or individual diagnostic success cannot repair the failed full parent, qualify a complete row/matrix or satisfy the original 24-hour gate. Completed raw evidence, actual executables and all three history models are independently accepted in [the complete diagnostic proof](accepted/). Both seeds pass all ten checkpoints on their first attempt; maxima are 18.675611114 / 19.351887878 seconds. These diagnostics did not reproduce the historical failures, and do not establish their cause.

The corrected-runtime full Tier1 normal100k and race1k gates are independently accepted. Existing real-cluster campaigns continue on their original handles. The historical TTL30s worker-kill mismatch is not rerun.
