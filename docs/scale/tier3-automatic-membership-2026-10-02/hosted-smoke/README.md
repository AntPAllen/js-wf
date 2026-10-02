# Verified R5 automatic-membership journal-fault smoke

[Hosted run36947230245](https://github.com/AntPAllen/js-wf/actions/runs/36947230245),
source `0bfc1b45a8b243ffc982ddb874db4f8c16b06b13`, passes in78.38s.
The workload lasts35s: five batches,140invocations,1542journal entries and one
actual journal-leader SIGKILL/restart. Current independent row, event and fencing
reviewers all pass. The worst terminal/progress p99 are13.069867495s/13.034888160s.
Histories, retained audits, immutable outcomes and physical queue drain pass.

Five production membership controllers and RunKVAssignments workers drive all
64partitions, balanced13/13/13/13/12. Actual R5/file/12s membership metadata and
coordinator renewal bracket the cut. All115acknowledged coordinator CAS writes
join to complete retained assignment revision chains. Lease acquisitions confirm
allfive workers execute real workloads. All104repair records acknowledge and
match final counters (78start/20signal/6suspended). Three fencing records match
counters and retained timelines; each completes after fencing within the
confirmed fault. These checks do not infer a server-side cause.

Every original artifact, complete hosted job log/API response and independently
rerun review is compressed losslessly. `original-sha256.json` records original
sizes and SHA256; every gzip was decompressed and byte-verified on retention.
This is stable five-member membership with one journal fault, not membership
churn, coordinator-process takeover, repeated5s reassignment, a sustained row,
200-seed acceptance or the full24h matrix.
