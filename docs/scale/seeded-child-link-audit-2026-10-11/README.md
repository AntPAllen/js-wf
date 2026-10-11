# Seeded retained child link audit — 2026-10-11

A new graph_child_link_audit workload writes canonical parent/child histories
through the real GraphStore on the seeded in-memory graph/start/signal/KV
transports. It retires the child, purges its invocation source, binds an actual
canonical signal, and retains either a consumed arrival or a rejected arrival
inside a failed terminal limit_entry. Raw snapshots clone complete root/fence/
projection censuses and load bounded physical objects. No production graph reader
supplies the audit verdict.

Three seeded choices cover consumed/rejected × JSON/protobuf × seven modes:
healthy, projection mismatch, parent mismatch, input mismatch, projection write
dropped before commit, projection acknowledgement lost after commit and stale
projection read. Start metadata corruption is a recorded test-only mutation of a
retired root; it is not an operation claimed to be possible through a healthy
NATS protocol. Other uncertainty/read faults use the existing transport model.

Dropped projection writes yield an unavailable terminal witness, with the start
link still verified. Lost acknowledgements retain verified projection bytes.
Mismatched/stale witnesses reject. Every generated schedule replays byte-for-byte;
every case restores only its injected fixture witness, re-audits with fresh
snapshots and verifies start/terminal links. After parent retirement, source/
projection deletion and60s virtual expiry, collection leaves zero owned objects
and references remain consistent. This fixture restoration is not a production
repair path.

The generation command passed1000 bodies/exact replay in20.745s and saved all28
combinations. Its executable was not captured. Final normal/race commands include
1000 generated bodies, all28 pinned exact replays and two existing shared
failure-trace persistence controls. Both final executables are captured live with
hash/build/command metadata. Prelaunch source/module/corpus/CI inputs remain
hash-bound. See command/executable JSON and logs; review.py executes the exact
new workflow guard against each log, parses YAML and verifies unchanged inputs.
Hosted CI acceptance is not claimed.

    python3 docs/scale/seeded-child-link-audit-2026-10-11/review.py

The corpus grows to162 seed families/922 pins. This new family is synthetic raw
history/auditor coverage, not production worker/SDK operation interleavings,
original child publication/lease causality, NATS conformance, exact cap exhaustion
or storage power proof. Full current162 normal/race/extended and original native
fault/scale/soak/retention/import/admission/rollout gates remain open. The running
full161 normal campaign is frozen at ec97015 and excludes this new family; four
older frozen race/scale campaigns also remain independent.

## Accepted focused results and guard controls

Final normal exit0 in15.722s and race exit0 in201.263s each cover1000 generated
bodies,28 exact pinned replays and two shared persistence controls. Both live
executables were captured;2054 source/module/corpus/CI inputs remain unchanged.
AST inventory has162 families; all corpus files total922. The exact new workflow
guard passed locally against both logs.

Two overlays disable only the start-link or exact terminal comparison in the
production raw auditor. Start guard removal semantically fails at seed1
(rejected/input_mismatch); terminal guard removal fails at seed5
(consumed/projection_mismatch). Both actual exits1 mean an otherwise invalid
history was accepted by the disabled auditor, not compilation or timeout.
Each original/altered source, unique anchor, overlay, log and saved failing trace
is retained. The shared minimizer verifies exact failure replay and completes
five reproductions for each. Mutation/minimizer executables were not captured;
these two focused audit controls are not the original six-invariant release
mutation campaign. The intact source was never changed by the overlays.
