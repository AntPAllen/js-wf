# Native retirement graph:116-workload100k acceptance

Run37002783868 completes successfully at exact
3184b6596e026c7bdf6f669f53d3329f8e64f406. Independent inspection accepts the
terminal run/job, actual package pass10,442.686s,160 top-level passes, two
documented trace-only skips,261 source pins and exact1..100000 execution of all
116 source-inventoried seeded workloads (11,600,000 bodies). Aggregate
11,800,033 schedules,178,646,687 choices and2,661,652,244 transport events.

The declared test inventory and regression paths independently match the
recorded Git revision. The AST inventory program from that revision runs against
its extracted simulator sources and matches the uploaded116-workload inventory.
The complete suite-result report regenerates byte-for-byte from original JSON
events. An initial AST invocation used the current workspace instead of the
extracted source root; its118-workload output is retained explicitly as
`rejected-workspace-seeded-inventory.txt` and is not acceptance evidence.

All137 original files, uploaded report/events/inventories/time/log, terminal
metadata, complete campaign log, extracted source files/hashes and independent
report are archived losslessly. Every member is SHA256-compared on readback and
recorded in `manifest.json`. Originals remain under
`/tmp/js-wf-tier1-116-100k-37002783868`.

This accepts the116-workload native retirement graph. It predates initial journal
metadata recovery and native-delete reply recovery, and cannot clear the current
118-workload gate. The117-graph37012116265 and current118-graph37028379931
remain separate live campaigns. Full fault matrices and24h soak remain open.
