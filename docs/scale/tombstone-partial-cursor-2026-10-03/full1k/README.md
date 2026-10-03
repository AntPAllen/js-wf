# Complete graph after tombstone prefix certification

Exact `4f11ca2957f6d3426a1dee227f0aadb91f23207b` passes in 147.900 s:
174 top-level passes, two documented trace-only skips, all 379 regression pins,
and all 121 scalable workloads complete seeds 1–1000 (121,000 bodies).
Aggregate fixed/repeated counts: 124,719 schedules, 1,806,737 choices,
27,606,244 transport events.

Independent review verifies all 1,126 clean before/after source hashes against
exact Git, retained normal executable/build settings, actual compiled inventory,
source-derived seeded-loop inventory and every pin path. Raw Go JSON regenerates
the suite report byte-identically. Binary SHA256:
`cda647683e49d42b3a1d0f0e7b444f4c5aa1d2c9b747de0da58b3f3013c679b0`.

All 18 archive members were reopened and SHA256 verified before atomic
publication. The archive includes actual executable, raw events, source hash
manifests, commands/contexts, inventories, reports, producer log and reviewer.

```sh
tar -xzf originals.tar.gz -C /path/to/empty-directory
python3 /path/to/empty-directory/review.py /path/to/empty-directory --repo /home/exedev/js-wf
```

This qualifies the complete normal 1k graph. Current-source full race/100k
are dispatched separately; real matrices, population bounds, 24h and original
scale requirements remain open.
