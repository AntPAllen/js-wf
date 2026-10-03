# Complete retained-binary full1k at the partial-prefix fix

Exact2f9416ba4062cfe5abdc1e66e69a4ec9e0093d43 passes135.312s with170 top-level
tests, two documented trace-only skips,283 pinned regressions and all121 scalable
workloads completing seeds1–1,000. The new partial-prefix cases are128 fixed seeds,
not another scalable workload. Aggregate123,183 schedules,1,798,545 choices and
27,196,095 events includes fixed/repeated cases.

The archive retains the actual normal executable, build settings/commands,
source before/after hashes, full compiled/seed/pin inventories, raw Go JSON,
suite report and independent reviewer. Every member was reopened and hashed
before atomic rename. Review verifies1015 source hashes against Git, exact
executable SHA/build settings and compiled inventory, source-derived seeded
inventory and exact report regeneration.

Extract into a fresh directory, then reproduce:

```sh
python3 review.py /tmp/restored-evidence --repo /home/exedev/js-wf
```

This is full1k normal qualification. Focused reconciler/model race proofs are
separate. Earlier comprehensive100k/full-race data keeps its earlier graph;
newer full graph gates and real matrices/24h remain open.
