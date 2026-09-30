# Hosted throughput comparison with controlled client placement

[Actions run 36778974153](https://github.com/AntPAllen/js-wf/actions/runs/36778974153)
passed both leader and follower jobs at candidate `9d7e6c0`, against fixed
production reference `4fa3119`. Each job has three alternating pairs of the
unchanged 10,000-hot / 100,000-parallel append workload. Both builds use the
same retained benchmark harness; the baseline production source stays fixed.

| Client placement | Hot median candidate/baseline | Parallel median candidate/baseline |
| --- | ---: | ---: |
| Leader | 1.003080 | 0.994610 |
| Follower | 1.001873 | 0.993820 |

The leader hot medians are 2,106/2,113 appends/s, versus follower medians
1,522/1,525. This reproduces both rate bands from the preceding uninstrumented
failure while controlling placement in each comparison. It supports the
confounder explanation; it cannot recover the missing old leader metadata.
The previous failed gate remains retained.

Each directory contains the complete downloaded job artifact: six reports,
logs, summary, shared harness sources and its checksum manifest. Local
verification recalculated all medians through `scripts/check-cas-throughput.py`,
validated all topology/count/runtime fields, and verified both harness file
hashes and each summary's manifest hash. The threshold remains 0.8 for both
hot and parallel medians in both placements. This validates the throughput
comparison, not the outstanding full fault-matrix release gates.
