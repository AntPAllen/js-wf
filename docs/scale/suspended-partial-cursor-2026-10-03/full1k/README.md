# Complete simulator graph after suspended prefix certification

Exact `27440daf4f7383b42ba764e34ea2cb808d886264` passes in 162.451 s:
172 top-level passes, two documented trace-only skips, all 307 regression pins,
and all 121 scalable workloads complete seeds 1–1000 (121,000 seed bodies).
Aggregate fixed/repeated cases produce 123,439 schedules, 1,799,569 choices
and 27,237,376 transport events.

Independent review verifies all 1,048 clean before/after source hashes against
exact Git, the retained normal executable and build settings, actual compiled
test inventory, source-derived seeded-loop inventory and all pinned paths.
The suite report regenerates byte-identically from raw Go JSON. Executable
SHA256: `fb8d7b8a927b9c326fe0c49eb89899f90c10880ddb26f0256d7b527234d3efee`.

Every one of the 18 archive members was reopened and SHA256 verified before
atomic publication. The archive includes the executable, source manifests,
commands/contexts, raw events, inventories, reports, producer log and reviewer.

```sh
tar -xzf originals.tar.gz -C /path/to/empty-directory
python3 /path/to/empty-directory/review.py /path/to/empty-directory --repo /home/exedev/js-wf
```

This qualifies the complete normal 1k simulator graph. Current-runtime full
race/100k, real fault matrices, population bounds and 24h remain open.
After all local Go jobs and review ended, the disposable Go build cache was
cleared when free disk reached 149 MiB, restoring 877 MiB. No retained originals
or failed physical stores were removed.
