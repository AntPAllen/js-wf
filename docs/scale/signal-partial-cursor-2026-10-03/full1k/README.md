# Complete simulator graph after the signal prefix correction

Exact source `175300af804a39ae3e1e4ce77409ec7ebad367a6` passes in
159.302 seconds: 171 top-level passes, two documented trace-only skips,
295 regression pins, and all 121 scalable workloads complete seeds 1–1000
(121,000 seed bodies). Aggregate fixed/repeated cases bring counts to
123,311 schedules, 1,799,057 choices and 27,231,567 transport events.

The independent reviewer verifies all 1,033 clean before/after source hashes
against exact Git, the actual retained normal executable and its build metadata,
compiled test inventory, source-derived seeded-loop inventory, all pinned paths,
and byte-identical regeneration of the suite report from raw Go JSON. Binary
SHA256 is `4161940015b8780fdbcb1fd778608e0c74546d4a3ff3cfbaa12bf3ae71733210`.

All 18 archive members were reopened and SHA256 verified before atomic
publication. The archive includes the actual executable, source hash manifests,
raw events, reports, commands, execution contexts, producer log and reviewer.

```sh
tar -xzf originals.tar.gz -C /path/to/empty-directory
python3 /path/to/empty-directory/review.py /path/to/empty-directory --repo /home/exedev/js-wf
```

This qualifies the complete normal 1k simulator graph. Full current-runtime
race/100k, real-cluster matrices, population bounds and 24-hour requirements
remain open. Hosted runs at `0d3fabf` cover the preceding Start correction.
