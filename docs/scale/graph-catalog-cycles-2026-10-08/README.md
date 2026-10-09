# Bounded cycles for every canonical repair scanner — 2026-10-08

Start, Signal and terminal catalog repair now share `graphCatalogCycle`. One authority watermark survives serial budgeted calls; each Scan captures its own immutable local bound. Wrap, uncertainty, changed checkpoint, rewind or process restart refreshes the bound. Quorum read witnesses and concurrent publications beyond that snapshot are deferred until the next cycle, preventing the scanner from indefinitely consuming its own observations.

The new deterministic control requires all three scanners to finish at least two complete two-destination cycles with one-record budgets. Neither destination needs repair. No logical-head mutation, payload access or dispatch is permitted. A counterfactual overlay using an unbounded watermark fails all three scanner variants. Existing unknown-prefix, prepared-append, dry-run, retirement, Signal-repair position and terminal recovery controls remain enforced.

## Development evidence

- Canonical reconciliation controls pass normal and race, including 48 cycle-wrap cases (three scanners × 16 seeds).
- Native R1/R3 Start, Signal and terminal fenced loops pass normal and race.
- Four affected shared families are exercised over 1,000 generated and exactly replayed seeds each; terminal catalog replay remains unchanged by the helper extraction.
- Forty-three saved pin migrations add only successful `graph_publication_catalog_high_water` observations. Every seed, decision, non-transport field and other transport event remains identical. The twelve captured terminal-catalog pins are byte-identical. Prior versions remain in Git; `development/pin-lineage.json` records hashes and added-observation counts. Total inventory remains 148 families/748 pins, with 705 prior pins unchanged.
- Complete pinned corpus passes normal and final race. The earlier race corpus failed its ten-second per-model CPU watchdog on two batch traces under concurrent qualifications. It is retained as failed evidence. The wall-clock schedule watchdog is now one minute; virtual transport/recovery targets and all domain assertions are unchanged. This does not change production RPC deadlines or the live frozen campaigns.

`development/` retains passing and failed results, counterfactual overlay, pin capture results and executed review. These are development controls; complete current frozen normal/race and extended acceptance remain separate requirements.

## Remaining scope

Watermarks schedule scanning and confer no payload or lifecycle authority. The cycle state is process-local and can safely refresh on leader restart; existing persistent scan checkpoints and per-generation Signal repair positions remain unchanged. Each scanner is driven serially by its fenced loop. This does not introduce atomic cross-stream lifecycle/state fencing, repair present stale/corrupt projections, migrate remaining runtime/history/CLI/deployment paths or enable production online GC.

Every original extended simulation, native process/storage/power-loss/concurrency/capacity/matrix/actual24h/million physical-drain/default-adoption/release requirement remains open. The live complete normal 148-family campaign at `7ee9882` and race 146-family campaign at `2b71f1d` exclude this later source change.

## Frozen complete normal qualification in progress

Source `55eeb77` is isolated at `/home/exedev/js-wf-catalog-cycles-qualification`:

```sh
python3 scripts/check-tier1-race.py --root /home/exedev/js-wf-tier1-full148-cycles-normal1000-20261008 --seeds 1000 --no-race
```

Every current family and pin is required. Terminal evidence and independent review are pending. The existing complete race campaign remains at its original frozen source.

## Frozen complete normal qualification accepted — 2026-10-09

The fresh complete normal run at `5aafc29` passes 210 groups, all 148 families × 1,000 contiguous completed bodies and all 748 pins in 649.324 seconds. `complete-normal1000/executed-review.py` verifies 3,052 selected inputs against Git and unchanged before/after, and retained binary/event integrity. This includes shared cycles and the distinct Signal failure diagnostic. Later worker CLI/reconciliation/read changes are excluded; complete race/extended and all wider original gates remain required. The prior interrupted `55eeb77` run remains preserved separately.
