# Full canonical purge/reuse run with Start phase diagnostics

Frozen `10dc1a7` includes the Start phase error labels and workflow/cohort/parent
context diagnostics. All 1232 Go/YAML/module inputs were copied directly from
that Git revision before compilation. The build closes actual exit 0 and all
snapshot bytes remain unchanged. The retained normal integration executable is
`d56475ee510a6669a9b309219251b5f0e1794a00d07b49a21fbfba28fef8d9f8`.

The same executable passes a separate **active-handler** 10/10/1 preflight,
actual exit 0 / 11.211 seconds. All ten live handlers execute before purge; its
raw audit has 11 terminals, 9 tombstones, 66 entries and zero reader pins.
The 2160 closed native files / 19,191,700 bytes verify independently. This is
diagnostic acceptance, not the full population requirement.

## Full campaign launched

The original full **10k completed / 10k active / 1k reused ID** campaign launched
at 08:43:38 UTC on the same executable, retaining 20-minute parent and 22-minute
SDK watchdog. No count override, PostgreSQL path or uncertain Start retry was
added. Unit `js-wf-canonical-purge10000-phases-20261011.service`, InvocationID
`d9d9dbf5b38c47f297bf9cd14ba9f6f7`, supervisor294508 / driver294509. Root:
`/home/exedev/js-wf-canonical-purge10000-phases-20261011`.

The immutable supervisor binds all source/binary bytes, exact command/environment,
actual child PID/birth/exit and complete closed native census. Launch verifies
the actual driver's executable and arguments, loaded unit and supervisor hash.
`prepared-review.py` independently accepts the active diagnostic and verifies
the live full process; `full-review.py` refuses live state with actual exit 1.
Live state/log/native files remain outside Git until closure. Do not restart this
campaign on an observation timeout. `terminal-review.py` retains terminal evidence
for either outcome and delegates successful full acceptance to the full-count
reviewer. A failed original store and the earlier copied recovery proofs remain
unchanged.

Admission requires 32 GiB free space. Service CPU quota is two cores, MemoryMax
4 GiB, driver GOMAXPROCS2/GOMEMLIMIT2GiB, engineering service lifetime30m.
Four independent race qualification jobs are also live on this four-core VM;
their combined quotas can exceed available CPU. `resource-context.json` records
the observed load, available memory/disk and other service limits. It does not
prove a timeout cause or establish a dedicated-host performance result.

Full acceptance, fault/soak/adoption/physical collection/power-loss requirements
remain open. This run may provide a qualified functional full-population result
or a specifically labelled failure; neither substitutes for the other gates.
