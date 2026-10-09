# Complete frozen deterministic qualification — normal passed, race watchdog failed

Frozen source: `fca8264d2229144e9b3e6a70df747d1307666fe6`, checkout `/home/exedev/js-wf-compaction-qualification`. This includes checkpoint compaction and the later authority guard.

The complete normal and race campaigns have been started, each requiring all 149 seeded families × 1,000 contiguous completed bodies and all 762 saved regressions. The complete normal campaign passed in 1,956.496 seconds, with independent source, binary, event and exact coverage verification. The race campaign now terminates with exit 1 at its 180-minute watchdog, after 89 of 149 seeded families completed. [Terminal failure and executed source/binary review](complete-race1000-timeout/README.md) retain its incomplete scope. The normal compiled inventory has 213 groups, with the two explicit trace replay/minimization utilities expected to skip. Running and partial evidence are not acceptance.

Retained run roots:

- Normal: `/home/exedev/js-wf-tier1-full149-normal1000-corrected-20261009`, supervisor tool session `91458`.
- Race: `/home/exedev/js-wf-tier1-full149-race1000-20261009`, supervisor tool session `68052` (terminal exit 1).

`run-normal.py` records exact commands and working directories, source manifests verified against Git blobs, retained binary provenance, JSON events and independent suite verification. The race campaign uses the frozen `scripts/check-tier1-race.py`, retaining its source/binary/command/context/event evidence. Both run the test binary from the frozen `sim` directory for saved regression discovery. The test-process watchdogs are 300 minutes normal and 180 minutes race; production deadlines and the seed CPU watchdog remain unchanged.

The first normal attempt was explicitly interrupted after discovering that its runner used the repository directory rather than `sim`; its partial events, nonzero exit, binary/source provenance and interruption reason are preserved in `initial-wrong-cwd/`. It is not a runtime defect or a qualified campaign. A subsequent runner syntax error was corrected before another process started.

The [complete normal evidence and executed review](complete-normal1000/README.md) qualify frozen `fca8264`, including compaction and the authority guard. They exclude later process fixtures, CLI cursor selection and Await retry changes. Full race acceptance, complete qualification of subsequent production changes and extended 10,000/100,000-seed campaigns remain open. Full simulation qualification does not discharge original native fault matrices, actual 24-hour soak, physical drain, migration, admission or release requirements.
