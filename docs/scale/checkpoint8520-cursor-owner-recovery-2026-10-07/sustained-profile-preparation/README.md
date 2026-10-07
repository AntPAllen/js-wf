# Sustained explicit recovery profile prepared

`run-tier3-soak.py --parallel-state-retained-audit` selects the same explicit ordered parallel/chunked/concurrent-state recovery checker for every cutoff checkpoint and final full audit. It requires4GiB and selects4CPU/GOGC500; the existing20s/60s checkpoint, original workload/fault schedule/duration, full retained history, latency/progress/queue/fencing checks and final-stage limits are retained. It is mutually exclusive with other readers and cannot activate from inherited WF environment. Default reader selection remains unchanged.

Actual SDK selector output identifies both checkpoint and final full selections. The independent row reviewer requires those actual markers, the recorded opt-in, matching4CPU/GOGC500/4GiB and no conflicting reader flags in addition to the existing raw sustained/provenance/archive checks. Three profile guard groups reject nine contradictory/omitted/bad selection variants and allow an extra empty checkpoint only alongside the required positive checkpoint/full selection. This is profile/selector evidence, not a replacement for raw row review or actual process/dependency admission.

All14 producer planner controls pass. Go original retained-budget/cancellation plus all ten reader-conflict pairs pass normal/race count5. Native sustained qualification is pending; the recorded1cd14c4 fixed-cohort peer-outage result remains scoped to that executed source, not blanket acceptance of this later integration change.

Next original ten-minute journal profile (fresh root, normal SDK):

```sh
python3 scripts/run-tier3-soak.py --root /tmp/js-wf-parallel-recovery-journal-ten-minute-20261007 --row journal --duration 10m --seed 1 --no-race --memory-limit 4GiB --parallel-state-retained-audit --retained-audit-trace --audit-wait-stack --explicit-route-seeds --bulk-final-latency --compare-bulk-point
```

Use the same actual supervised handle through terminal state, then independently bind actual SDK/server/dependency profiles, all raw artifacts and full archive before accepting the row. A ten-minute row still does not qualify full matrices or the actual24h gate. Original failed stores and failed verdicts remain preserved.
