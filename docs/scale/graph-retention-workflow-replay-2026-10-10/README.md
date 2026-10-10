# Canonical retention SDK crash/reuse simulation

The canonical retention handler now shares its durable SDK implementation between
JetStream and the existing narrow GraphPurgePort. The production JetStream lookup
and purge calls retain their original deadlines, retry limits and recorded target
generation. The port adapter allows deterministic execution of that same handler.

## Coverage

The model seeds a completed canonical target with a graph-owned result, runs the
production handler through wf.Run, interrupts before each of its four append
commits (lookup requested/completed, purge requested/completed), and replays the
retained entries in a fresh SDK context. Seven modes include a clean run and ID
reuse after the target lookup has been recorded at the two purge boundaries.
The reused generation must survive unchanged and the old purge must return stale.
Independent reference checks and exact transport trace replay run in every mode.

The focused SDK test covers 16 seeds per mode (112 executions and 112 replays).
The complete Tier 1 inventory adds one 1,000-seed family, with exact replay for
every generated schedule, bringing the inventory to 159 families. Seven saved
traces bring the corpus to 860; all prior 853 files are byte-identical to the base.
Fixture creation is direct graph construction, with compatibility terminal state;
this does not qualify a full worker, canonical-start mode or native crash recovery.

## Evidence and limitations

- `development.json` and `development-race.log`: full retention package race
  before extracting the SDK corpus into sim, actual exit 0 (38.866 seconds).
  `original-sdk-cut-corpus.go.txt.gz` freezes that exact earlier test source.
- `integrated-source.json`: hashes for the integrated worktree, observed during
  compilation; this is development evidence, not a clean revision qualification.
- `integrated-review.json` records actual exit 0; `integrated-race.log`: focused integrated SDK family, all pinned regressions,
  SDK cuts and existing native R1/R3 healthy retention handler tests.
- `mutation-review.json`/`mutation.log`: a shared-handler mutation re-looks up
  the generation on retry; both reuse modes detect replacement deletion. The
  mutation uses a Go overlay and is never installed in the working source.
- `integrated-mutation.log`: the same control repeated after integration into sim.
- `complete-run.py`/`complete-review.py`: source-bound complete 159-family normal
  and race qualification, actual supervisor/child exits, complete source/binary
  verification and 860-pin/159,000-body checks. Race requires accepted normal.

The separately frozen b3d0cb1 complete normal suite passed 158,000 seed bodies
and 853 traces (see ../graph-current-tier1-2026-10-10/normal-review.json).
Its source predates this change. Full 159-family normal/race and extended seed
qualification remain open until their own actual runs and independent reviews.
Original native scale/fault/soak/storage/retention/import/security/rollout gates
remain separate; admission and online collection remain disabled.
