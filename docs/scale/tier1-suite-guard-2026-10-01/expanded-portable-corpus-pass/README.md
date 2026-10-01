# Expanded portable corpus complete-suite proof

At clean source53897143d059ba40734d1367c06202782518c6cf the complete
Tier1 simulator suite with SIM_SEEDS=1000 passes107.721s (108.21s wall).
The strict suite-result guard passes:150 top-level tests,two documented
trace-only skips and all180 source-inventory pinned regressions.

Coverage records103,632 generated schedules,1,738,227 scheduler choices and
24,098,821 transport events. This proves complete compiled-inventory execution
and all pins at this source; configured seeds are not independently verified
per-workload seed coverage. It is not the100,000-seed release gate.

Original source,compiled inventory,pin inventory,Go JSON events,rendered
output,elapsed time and guard report are retained. Large files are
deterministic gzip; artifact hashes refer to uncompressed bytes.
Systemd unit js-wf-tier1-portable-20261001 completes inactive/success.
No stale or interrupted prior result is included.
