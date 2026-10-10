# Tier 1 canonical running cancellation polling — 2026-10-10

The seeded in-memory transport runs the production GraphStore, canonical client
and Worker.PollRunningCancellation. Seven modes cover absent and reserved input,
committed binding after source removal, foreign key/generation and uncertain
root/owned reads. Uncertain reads fail closed and retry recovers the same binding.
Every generated trace must replay exactly. This isolates polling decisions;
restored effect abandonment, retry and terminal publication have separate native
[eight-case evidence](../graph-continuation-running-effect-cancel-2026-10-10/review.json).

[Development receipt](development-receipt.json) records 1,000 race seeds and exact
replays, all seven new pins and three required failures when canonical polling
is disabled. Initial trace-directory failure is preserved; creating the requested
output directory repaired the test harness. The suite inventory is now 157 seeded
families and 848 saved traces. Older full156 evidence remains scoped to its source.

The frozen [runner](run.py) will retain normal/race binaries and run 1,000 race
seed bodies, all 848 normal pins, three required mutant failures and 100,000 normal
seed bodies. [Reviewer](review.py) requires actual terminal supervisor exit0,
exact source/binary identity, complete body/mode counts and every pinned result.
No frozen or extended acceptance is claimed before closure and review. Full
latest157 normal/race/extended, native fault, retention, import, public admission,
production collection, rollout and every broader original requirement remain open.

Frozen source `ecc78eb811f7f556a65c2b972948042a7b18024e` is confirmed live under `js-wf-graph-cancel-poll-20261010.service`.
[Launch identity](launch.json) records the process and retained invocation.
