# Full-suite race CPU watchdog and command exit evidence

The frozen full-149 race campaign exhausted its 180-minute test watchdog after 89 families; that terminal failure remains preserved and unqualified. The later directed 336-combination race group also adds 1,336.887 seconds of actual workload. A diagnostic batch trace shows substantial race-instrumentation overhead; it does not establish an exclusive whole-campaign cause.

Future full race test processes use a 300-minute wall-clock CPU watchdog, with a 320-minute CI job budget. Normal already uses 300 minutes. The compiled inventory, canonical seeded-family inventory, 1,000 contiguous bodies per default family, every saved regression, per-schedule watchdog, virtual transport deadlines, production recovery targets and all property assertions remain unchanged. No partial earlier campaign is reclassified as passing. Extended normal seed configurations retain their existing scope/budget.

The runner writes `command-results.json` before propagating every completed child's failure. [Executed actual-child control](executed-command-exit-control.py) invokes `/bin/true` and `/bin/false` through the extracted runner function, verifies actual exit codes 0/1 and cwd, and confirms failure propagation. This is a harness control, not distributed/runtime qualification.
