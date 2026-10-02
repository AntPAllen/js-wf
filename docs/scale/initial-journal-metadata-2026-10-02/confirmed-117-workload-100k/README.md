# Initial journal metadata graph: 117-workload 100k acceptance

[Run 37012116265](https://github.com/AntPAllen/js-wf/actions/runs/37012116265)
finished successfully at exact `82f323a593acef65b75102e6bfc9318d1caa0c8a`.
Independent checks accept the actual package pass in 14,259.586 seconds,
161 top-level passes, two documented trace-only skips, 262 source pins and
100,000 contiguous seeds for each of 117 workloads: 11,700,000 test bodies.
Aggregate coverage is 11,900,033 schedules, 178,746,687 scheduler choices and
2,663,852,114 transport events.

The compiled test inventory was regenerated from extracted Git sources.
The seeded-loop AST inventory and regression paths independently match that
revision. The suite report regenerates byte for byte from the original Go JSON
events. An initial AST command ran from the current workspace; its output is
retained as `rejected-workspace-seeded-inventory.txt` and excluded from acceptance.

All 847 originals, including uploaded events/report/inventories, terminal job
metadata, complete campaign log, extracted source and independent checks, are
archived losslessly. Every member's SHA256 was checked on readback before atomic
archive publication. See `manifest.json`; originals remain in
`/tmp/js-wf-tier1-117-100k-37012116265`.

This clears the 117-workload graph's seed gate. It predates the native-delete
reply and delivered-source-retention workloads and does not qualify the current
119-workload graph, full fault matrix, million-timer drain or 24-hour runtime soak.
