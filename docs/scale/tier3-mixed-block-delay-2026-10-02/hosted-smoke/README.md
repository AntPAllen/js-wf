# Verified hosted dm-delay smoke

[Run 36945872372](https://github.com/AntPAllen/js-wf/actions/runs/36945872372),
source `246ed70e0eedc76857163dac51d5249891b76484`, passes its native capability
test (6.62s) and mixed row (81.96s). The workload interval is 35s: nine batches,
252 invocations, 2,775 journal entries and one admitted fault. Independent current
row, event and fencing reviewers pass. Worst terminal/progress p99 are
5.004610663s / 0.490248438s. Histories, retained invariants, immutable outcomes,
physical queue drain and repair-counter reconciliation pass; all 41 repair
records are acknowledged and no fences are observed.

Both R5 dispatch and journal leaders are on the actual writable private store.
The observed table is `0 1048576 delay 7:0 0 100`; the delay remains active for
5.000764980s. The dirty sync takes 414.745020ms, exceeding the requested 100ms.
The restored table is `0 1048576 linear 7:0 0`, on the same backing device,
before confirmed R5 heal. Native capability also checks cancellation restoration,
a usable subsequent write, and mount/image cleanup.

Every original download, complete hosted job log, API response and independent
review output is retained as gzip. `original-sha256.json` records uncompressed
size/hash; all entries were decompressed and verified before committing.
This is a single smoke cut, not sustained, combined-fault or full Tier3 acceptance.
