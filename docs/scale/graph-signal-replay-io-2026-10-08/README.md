# Canonical Signal replay body reads — 2026-10-08

## Change

`GraphView.SignalBindingAt` validates the held queue generation and descriptor without fetching its payload. Already consumed Signals use this descriptor to match the canonical index/token/sequence/name/hash, then fetch and validate the separately journal-owned body once. Fresh consumption still uses `SignalAt` and copies its body into journal ownership.

Replay requires the exact `graph-signal-<hash>` reference and rejects inline payloads. Native R1/R3 hostile-history controls now include foreign references, inline-only bodies and inline bodies accompanying an otherwise valid reference. These controls supplement missing canonical identity and index/token/sequence/name/hash mutations.

## Development evidence

- The production-worker model control passes with exactly one input-body GET. Blocking that GET prevents handler entry and completion after source purge. The original implementation overlay fails the healthy control with two GETs; its denial control passes.
- The changed runtime passes 1,000 generated and exactly replayed Signal runtime schedules in `development/capture-1000.jsonl` (114.998 s), with all eighteen controlled modes. This is development evidence, not a frozen complete-suite qualification.
- Eighteen trace migrations preserve seed decisions and every non-transport field. The only removed transport events are successful redundant queue-body GETs: 380 in the batch/restart trace, three in consumption-unknown, and two in each other mode. Prior bytes remain in Git at `04f8dbd`; `development/pin-lineage.json` records both hashes. The other 710 pins are unchanged.
- Normal and race worker/journal/pinned-corpus controls are recorded in `development/final-*.jsonl`; the executed review checks terminal results and exact test coverage.
- Earlier import-cycle build failure and invalid fixture runs remain preserved. Fixture corrections supply the actual dispatch partition and canonical Started ownership metadata. No runtime defect is inferred from those fixture failures.

The existing full default race campaign remains isolated at `dd98e39`; these working-tree changes do not alter its source or trace bytes. It passed its 1,000-seed Signal runtime group in 1465.960 s, but complete-suite terminal evidence is still pending.

## Complete normal qualification in progress

The complete default normal suite runs at frozen `7626120` from `/home/exedev/js-wf-signal-replay-io-qualification`, retaining its binary, source inventories and results under `/home/exedev/js-wf-tier1-full145-replay-io-normal1000-20261008`. Command:

```sh
python3 scripts/check-tier1-race.py --root /home/exedev/js-wf-tier1-full145-replay-io-normal1000-20261008 --seeds 1000 --no-race
```

The full 145-family/728-pin result and independent review are pending. The other full race campaign remains at its original source; it does not qualify this later change.

## Scope still open

This component reduces replay I/O and reinforces canonical ownership. It does not qualify independent fault combinations, all extended seed campaigns, VM/process/power-loss/network/storage faults, runtime state/timer/snapshot/continuation/import/discovery/deployment migration, full native matrices, the actual 24-hour soak, million-timer physical drain, default adoption or release. Canonical modes remain opt-in and production online GC remains disabled.
