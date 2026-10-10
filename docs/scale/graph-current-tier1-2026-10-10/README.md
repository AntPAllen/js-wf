# Complete current Tier 1 qualification

Frozen `b3d0cb16ff3350db298336e22bf87c5aadf46439` contains the subsequent
grant-order, durable continuation, compaction lifetime and terminal visibility
fixes missing from the historical `0404fc0` full-suite evidence. The complete
normal suite is running; this directory does not establish acceptance yet.

[Source](source.json) selects 3,503 tracked inputs in a clean isolated sparse
checkout. The retained binary enumerates 227 tests, the AST inventory finds
158 seeded families, and the regression inventory contains 853 saved traces.
Each family runs 1,000 seeds. Eight disjoint package processes run sequentially
from the same binary with their original 300-minute per-process watchdogs.

[Launch](normal-launch.json) binds the actual service invocation, supervisor
and driver PIDs, command and supervisor hash. `GOMAXPROCS=2` and
`GOMEMLIMIT=512MiB` are unchanged; the service has an explicit **one-core CPU
quota** so the native entry campaign can run concurrently. This is a recorded
resource condition, not a claim of the prior unrestricted CPU profile.
Workflow timing targets and fixture bounds are unchanged.

The first launch had a mistyped source SHA and stopped at the checkout source
assertion before creating a child or running any tests. Its
[terminal properties](development-preflight-unit.txt) and
[journal](development-preflight-journal.log) are retained. The corrected launch
uses a distinct unit and the exact SHA from `source.json`.

After actual terminal completion, run:

```sh
python3 docs/scale/graph-current-tier1-2026-10-10/review.py normal
```

[Review](review.py) requires matching loaded terminal service and actual child
exit zero, recorded CPU quota, exact Git inputs before/after, binary identity,
compiled/seeded/pin inventories, eight exact selectors, eleven successful
commands, 158,000 completed seed bodies, and independent checker agreement.
Raw outputs remain under `/home/exedev/js-wf-current-tier1-20261010/normal1000`;
the mutable state file is operational evidence and is not committed as a
terminal receipt. Observation timeouts do not authorize a restart.

Race requires independently accepted normal and a separate launch receipt.
Current race, extended 10,000/100,000 seed runs, original native fault matrices,
actual entry cap, storage/VM faults, soak/drain, security, retention, admission
and rollout remain separate open gates. Admission and collection remain off.
