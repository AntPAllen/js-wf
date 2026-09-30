# Focused production mutation checks

Run from the repository:

```sh
python3 scripts/check-invariant-mutations.py --output /tmp/js-wf-invariant-mutations
```

The runner uses Go overlays to change production code without editing the
checkout. Each selected fixture must first pass unmodified. A mutant counts as
detected only if that exact test fails with the required semantic evidence.
Compilation failures, skipped tests, suite timeouts and unrelated failures do
not count. Two mandatory runner controls verify rejection of a compile error
and an unrelated test failure. Source anchors must occur exactly once; a source
change that invalidates an anchor fails the runner.

| Mutation | Production change | Required detection |
| --- | --- | --- |
| Missing journal CAS | Remove the expected subject sequence publish option | Two acknowledged winners in one gated append race |
| Independent worker leases | Give each worker a private lease key for the same invocation | Second worker incorrectly acquires while the first holds ownership |
| Reversed retirement order | Purge the invocation before its journal and terminal tombstone | Crash-boundary purge retry cannot recover the invocation |
| Missing determinism guard | Disable step kind/name/input comparison | Changed replay step is accepted without the required divergence error |
| Missing run message ID | Remove the enqueue deduplication publish option | 64 equal-ID concurrent enqueues retain 64 messages instead of one |
| Skipped start reconciler | Return without scanning or repairing starts | A retained invocation's absent wakeup remains unrepaired |

The CAS fixture runs 1,000 races and two journal-leader restarts. CAS, lease and
enqueue fixtures use real three-node clusters; retirement uses a real
single-node crash-boundary fixture. Replay and start repair use production code
with seeded in-memory transports. This is stronger than supplying a forged bad
history, but it is **not the full mixed-workload six-mutation chaos release gate**.
The independent full matrix, seed-count and soak requirements still apply.

The output directory contains `report.json`, unmodified and mutated Go JSON
test logs, negative-control logs and any modeled failure traces. The report
records the commit, production source hashes, selected tests, timings and
classification. A single category can be selected with `--mutation`; this does
not count as the complete six-category pass.

The `invariant-mutations` GitHub workflow runs manually or when relevant
production code, selected fixtures or the runner change on `main`. It always
uploads the output directory, including on failure. A survivor, fixture failure
or missing expected semantic marker fails CI.
