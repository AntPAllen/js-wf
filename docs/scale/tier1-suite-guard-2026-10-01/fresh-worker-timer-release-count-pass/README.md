# Independently reviewed complete simulator campaign at57ecf06

[Run36942165713](https://github.com/AntPAllen/js-wf/actions/runs/36942165713)
completed successfully at exact source
`57ecf06ed179ae2083a65b13453ccbf85eafc1ab`, configured with100,000 seeds.
The current independent suite reviewer reproduces the original report exactly:
151 top-level passes, two documented trace-only skips and all184 pinned
regressions. The downloaded regression inventory matches that commit's actual
Git tree. Counts are10,400,632 generated schedules,173,756,738 scheduler choices,
2,435,250,750 transport events, maximum virtual time7,200,000ms.
Measured elapsed time is12,682.31s; the full Go events, rendered suite log,
full job log, API state, inventories, timing, source and both reports are retained.
All11 original files are copied or deterministically compressed; uncompressed
SHA256 hashes and byte lengths are retained and verified.

```sh
gzip -dc tier1-events.jsonl.gz > /tmp/tier1-reviewed-events.jsonl
python3 scripts/check-tier1-suite.py --events /tmp/tier1-reviewed-events.jsonl \
  --inventory tier1-inventory.txt --regressions tier1-regression-inventory.txt \
  --source tier1-source.txt --seeds 100000 --output /tmp/tier1-reviewed-result.json
```

This is complete compiled-inventory evidence at the recorded source, including
the fresh-timer production-worker workload. Aggregate seed configuration does
not independently prove per-workload seed coverage. It predates canonical SDK,
shared clock/provider, common-clock transitions and supervised membership
recovery; the current234-pin corpus and final-source100k remain open. It also
does not replace native fault, scale, mutation or24-hour gates.
