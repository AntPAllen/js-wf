# Complete current deterministic qualification — pending

Frozen source: `fca8264d2229144e9b3e6a70df747d1307666fe6`, checkout `/home/exedev/js-wf-compaction-qualification`. This includes checkpoint compaction and the later authority guard.

The complete normal and race campaigns have been started, each requiring all 149 seeded families × 1,000 contiguous completed bodies and all 762 saved regressions. Terminal verdicts remain pending. The normal compiled inventory has 213 groups, with the two explicit trace replay/minimization utilities expected to skip. Running and partial evidence are not acceptance.

Retained run roots:

- Normal: `/home/exedev/js-wf-tier1-full149-normal1000-corrected-20261009`, supervisor tool session `91458`.
- Race: `/home/exedev/js-wf-tier1-full149-race1000-20261009`, supervisor tool session `68052`.

`run-normal.py` records exact commands and working directories, source manifests verified against Git blobs, retained binary provenance, JSON events and independent suite verification. The race campaign uses the frozen `scripts/check-tier1-race.py`, retaining its source/binary/command/context/event evidence. Both run the test binary from the frozen `sim` directory for saved regression discovery. The test-process watchdogs are 300 minutes normal and 180 minutes race; production deadlines and the seed CPU watchdog remain unchanged.

The first normal attempt was explicitly interrupted after discovering that its runner used the repository directory rather than `sim`; its partial events, nonzero exit, binary/source provenance and interruption reason are preserved in `initial-wrong-cwd/`. It is not a runtime defect or a qualified campaign. A subsequent runner syntax error was corrected before another process started.

Current full normal/race verdicts, independent executed review and extended 10,000/100,000-seed campaigns remain open. Full simulation qualification does not discharge original native fault matrices, actual 24-hour soak, physical drain, migration, admission or release requirements.
