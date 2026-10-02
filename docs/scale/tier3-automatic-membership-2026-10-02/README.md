# Automatic membership in the R5 mixed journal-fault fixture

The `auto_journal` row registers five actual production membership controllers
against R5 `WF_MEMBERS`, then runs each production worker's `RunKVAssignments`
watcher. No static partition loop or manually assigned owner drives this row.
The controllers claim and balance all64 partitions; the same mixed workflow
cohorts then run during journal-leader SIGKILL/restart every30seconds.
The common histories, raw<30s p99, retained audits, immutable terminals and
physical dispatch drain stay required, with production2m file sync.

Before workload/fault admission and after the fault run, the fixture records
actual live membership, all64 retained owner/revisions, the retained coordinator
lease and actual membership-stream metadata/current R5 replicas. It requires
13/13/13/13/12 ownership and a live positive coordinator epoch. Every coordinator
assignment call records its supplied CAS revision, returned revision, caller,
owner and controller timestamps. Membership controllers and assignment watchers
are joined before coordinator/registration release on cleanup.

The independent row guard joins successful coordinator writes into every
partition's CAS chain from initialization through its final owner/revision,
checks coordinator renewal, the actual12s membership TTL/file storage/R5,
fault-bracketing snapshots and observed lease acquisitions on all five workers.
Seventeen artifact cases reject incomplete membership/partitions/writes,
unbalanced ownership, absent coordinator, bad replication/TTL, stale renewal,
offline replicas, wrong CAS, repeated revisions, unknown writes, mismatched final
owners, wrong callers, missing workers and reversed observation boundaries.
All37 Tier3 Python tests and both controller tests pass. The new native row
compiles under race; it skips without the explicit matrix opt-in and supplies
no native acceptance yet. A clean hosted35s smoke is the next gate.

This row exercises automatic controllers/watchers with journal faults and five
stable members. It does not claim membership churn, paused/isolated/killed
coordinator takeover, worker/server clock combinations, or a reassignment every
five seconds. Those combined automatic-membership fault cases and the200-seed
and24-hour full release matrices remain required.
