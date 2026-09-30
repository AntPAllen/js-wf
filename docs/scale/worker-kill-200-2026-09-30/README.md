# Two hundred consecutive sustained worker-kill seeds

[Actions run 36701632166](https://github.com/AntPAllen/js-wf/actions/runs/36701632166)
completed successfully at `1cc9a39d7618ac65c60ea8a0feebcf28d72b5fa4`.
All 200 seeds ran the ten-minute mixed generator with real worker-process
SIGKILLs every five seconds. This is one fault row at that revision; it does
not establish the full matrix or validate later runtime changes.

`jobs.json` is the terminal `gh run view --json status,conclusion,headSha,jobs`
response. `logs.zip` is the complete Actions log archive from the run's logs
API. `report.json` is generated from those files, with their SHA-256 hashes.
The checker verifies every seed's release duration, selected test PASS, active
worker fault, workload composition, retained-count agreement and aggregate,
six workload terminal, and six workload progress p99 gates. Test PASS follows
history, final-heal completion and run-queue drain checks in the source. The
checker verifies recorded results; it does not independently inspect raw stores.

```sh
python3 scripts/check-matrix-campaign.py \
  --jobs docs/scale/worker-kill-200-2026-09-30/jobs.json \
  --logs docs/scale/worker-kill-200-2026-09-30/logs.zip \
  --row worker_kill --test TestMixedMatrixRandomWorkerKilledEveryFiveSeconds \
  --seeds 200 --output /tmp/worker-kill-200-report.json
```

Totals: 359,744 terminal invocations, 3,969,579 journal entries, 23,800 kills.
Worst aggregate terminal p99: 15.028 seconds. Worst workload terminal p99:
28.020 seconds. Per-seed progress measurements, including tails beyond p99,
are retained in the report. There were 5,188 kills of workers with active leases.
Two individual next-entry delays exceeded 30 seconds (maximum 46.030 seconds);
all progress p99 values passed. This is not a claim that every individual delay
was below 30 seconds.
