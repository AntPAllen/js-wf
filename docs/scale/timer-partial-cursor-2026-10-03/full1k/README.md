# Complete simulator graph after both timer prefix fixes

Exact `e6072da39aa751fec1a4903910d84da2bb900904` passes in 158.434 s:
173 top-level passes, two documented trace-only skips, all 355 regression pins,
and all 121 scalable workloads complete seeds 1–1000 (121,000 bodies).
Aggregate fixed/repeated cases produce 124,463 schedules, 1,805,713 choices
and 27,500,409 transport events.

Independent review verifies all 1,099 clean before/after source hashes against
exact Git, retained normal executable/build metadata, actual compiled test
inventory, source-derived seeded-loop inventory and every pin path. Raw Go JSON
regenerates the suite report byte-identically. Executable SHA256:
`d69f0248c4a29d3c25ae76613cf78144685eef2b7f34f740b54b6a24276e49cc`.

All 18 archive members were reopened and SHA256 verified before atomic
publication. The archive includes actual executable, raw events, source hash
manifests, commands/contexts, inventories, reports, producer log and reviewer.

```sh
tar -xzf originals.tar.gz -C /path/to/empty-directory
python3 /path/to/empty-directory/review.py /path/to/empty-directory --repo /home/exedev/js-wf
```

This qualifies the complete normal 1k graph. Current-source full race/100k,
real matrices, population bounds and 24h requirements remain open.
