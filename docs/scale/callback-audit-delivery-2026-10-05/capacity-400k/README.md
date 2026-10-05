# Full400k callback capacity failure

Native gate at b943074 fails97.60s. Same four-core/GOGC200/2GiB profile, R5 file
stores, exact400k invocations /4.8M journal entries /400k states and original20s
per-attempt deadlines. The million-timer diagnostic shares the VM; the workflow
dataset is quiet but this is not an isolated-machine benchmark.

| Mode | Elapsed | Allocated bytes | GC cycles | Verdict |
| --- | --- | --- | --- | --- |
| SDK concurrent | 20.000050691s | 5,015,732,616 | 26 | Deadline |
| Compact concurrent | 20.000192587s | 3,084,729,656 | 12 | Deadline |
| Callback concurrent | 20.000072258s | 2,657,940,080 | 19 | Deadline + cleanup join deadline |
| SDK recheck | 20.000700383s | 4,716,156,720 | 24 | Deadline |

The callback explicitly reports inability to join before the expired caller
deadline; it does not claim synchronous cleanup success in that failed attempt.
Actual observed SDK/five servers are closed after the native test. Partial reduction
counters are not evidence of missing stored data. Incomplete allocation totals
do not establish per-record throughput. Correctness controls do not qualify capacity.

Independent review verifies676 selected Git inputs, actual SDK/five server bytes/
modules/mounts/closure, initial copy hashes and1058 unchanged original files.
This does not cover every external compiler input. Complete closed-fixture archive
preservation is running under the tracked reviewer service. Archive acceptance
is pending until metadata exists and that service exits successfully.

No default adoption, server-defect attribution or24h qualification. Next diagnosis
should measure integrated delivery/reduction costs and eliminate remaining per-record
overhead while keeping original cardinalities/deadlines.
