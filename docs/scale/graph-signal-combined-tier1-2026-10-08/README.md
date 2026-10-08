# Independent canonical Signal fault combinations — 2026-10-08

The shared production client/repair/worker/terminal/fixture-GC runner now has a second workload, `graph_canonical_signal_runtime_combined`. Each seed independently chooses four dimensions:

| Dimension | Choices |
| --- | --- |
| Publication/initial cut | healthy, source drop, source lost acknowledgement, queue drop, queue committed with lost readback, reserved input, 19-input batch/restart |
| Discovery | healthy, catalog read drop, lifecycle read drop, dry scan |
| Enqueue | healthy, drop, lost acknowledgement |
| Consumption | healthy, lost acknowledgement, committed with lost readback, repair during a prepared append |

Repair reopens the graph/client/scanner after uncertain outcomes without caller payload or a local repair cursor, within eight attempts. All selected source/authority/enqueue fault queues must be empty at completion; queue and consumption interception must fire when selected. Dry scans cannot enqueue. Workers consume after first sources are purged, reopen for a second Signal, replay terminal deliveries, produce one side effect, preserve the exact consumed prefix, match terminal projection, write no legacy journal and completely drain fixture objects and dispatch. Prepared repair must run exactly once without a graph-head conflict. Existing single-recipe behavior and saved traces remain intact.

## Development evidence

`development/combined-normal1000.jsonl` passes 1,000 generated schedules with exact replay in 276.868 s, covering 320 of 336 Cartesian combinations and every marginal choice. Three saved compound traces cover a batch plus catalog/enqueue/consumption cuts, a source/lifecycle/prepared-repair combination, and reserved input plus enqueue/consumption cuts. `development/pins.json` records seeds, dimensions and hashes.

All 728 prior pins remain byte-identical to `44fe6bf`; the corpus now contains 731 pins and the seed inventory has 146 families. Normal and race corpus results are recorded in `development/pins-*.jsonl`. Executed review checks terminal results, exact pin coverage, unchanged prior pins and marginal coverage. This development result is not a frozen complete 146-family qualification.

CI includes the combined family with 1,000 seeds, completed-seed diagnostics and a 60-minute test/65-minute job CPU budget. Future complete race runs use 180-minute test/195-minute job budgets: the earlier 145-family campaign already consumed nearly its original 60-minute limit before this additional family. The existing campaign failed its original 60-minute alarm during suspended-scan capacity testing, with its source and failure retained; no recovery target, seed count or domain assertion is relaxed.

## Frozen complete qualification in progress

Source `2b71f1d` is isolated at `/home/exedev/js-wf-signal-combined-qualification`. The complete normal default campaign passes; the race campaign remains active:

```sh
python3 scripts/check-tier1-race.py --root /home/exedev/js-wf-tier1-full146-normal1000-20261008 --seeds 1000 --no-race
python3 scripts/check-tier1-race.py --root /home/exedev/js-wf-tier1-full146-race1000-20261008 --seeds 1000
```

Each requires all 146 families × 1,000 contiguous seed bodies and all 731 pins. Binary/source/event retention is automatic. Normal passes all 146 families × 1,000 contiguous seed bodies, all 731 pins and 207 groups in 767.138 s. `complete-normal1000/` contains executed independent review of 3,019 selected inputs against Git and unchanged before/after, exact coverage and retained binary/event integrity. Full race terminal evidence remains pending; this acceptance does not include later source changes.

## Remaining work

Sixteen combinations were not sampled by the first 1,000 seeds. Exhaustive combinations, arbitrary operation permutations, extended 10,000/100,000 campaigns, complete current normal/race qualification, and all original native/process/storage/power-loss/concurrency/capacity/matrix/24h/million physical-drain/runtime migration/default-adoption/release requirements remain open. Fixture collection does not enable production online GC. The combined family only expands Signal coverage; it does not complete the full implementation plan.
