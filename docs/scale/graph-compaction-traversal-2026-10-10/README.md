# Compaction source traversal and original grant checks

Production preparation now reads the captured source forest in one ordered retainedgraph.ReadRange traversal instead of a fresh root-to-leaf lookup per record. Both preparation and commit reuse the already authenticated source record edge for a private original-grant check. Public owned-payload callers still perform membership lookup. Every original fence/receipt/destination/expected-head/location check remains fresh; no grant or node cache is introduced. Shared payloads with an origin location at another index still prove that location's membership. Original-head publication CAS, independent relocated receipts, new-grant checks, reader preservation and archive collection contracts remain unchanged.

Development race tests pass for existing graph compaction controls and six new grant-revocation cases (before prepare/commit, missing origin, foreign destination and invalid origin location). Bypassing just the original-grant helper causes all six cases to fail by accepting invalid compaction. Exact mutant and logs are retained here.

The production journal prefix-cost test still passes all rejection/pin-release/retry/checkpoint/compaction assertions. Total compaction Gets at 36/260/1028 entries change from 1599/17291/83091 to 1039/10837/51289 (about35–38% fewer). Entry payload reads and initial checkpoint verification counts stay unchanged. These are transport operation counts, not native RPC counts or latency acceptance. Tests run alongside other work; wall-time comparisons are not performance claims.

All14 compaction pins originally diverged on the changed Get sequence. Their exact old JSON is preserved compressed. Refresh required unchanged seed, version, workload, step limit, decisions and every non-Get transport event; only the object-read sequence changed. [Independent pin comparison](pin-review.json). Remaining839 source pins are untouched. The running full normal/race campaign remains frozen at0404fc0 and cannot qualify this later optimization.

This does not solve the first checkpoint prefix scan, long publication deadline/reader-intent lifetime design, actual100000-entry cap or original scale/latency gates. Production collector remains off and public continuation admission closed.

The R1/domain/archive budget20 native development test passes under race with two checkpoints,20 ordered records, terminal slot19 and zero forbidden effects. Native phase observations are retained, with no latency comparison or actual-cap acceptance inferred. All853 refreshed pins pass in13.393s. A1000-seed compaction race development run is live in the current execution session; acceptance requires its actual completion.

## Frozen qualification live

Source `2de6dfa93184b4489436bf55f29179684e4e08d2`, detached clean sparse checkout `/home/exedev/js-wf-compaction-traversal-qualification`. Unit `js-wf-compaction-traversal-qualification-20261010.service`, original PID131436, invocation `c56c65f782b14e1ca8f024b2d3d2d57a`. [Launch](qualification-launch.json). [Driver](run.py) retains exact source inventories, four compiled binaries, actual command exits and raw test2json events. Resource profile is GOMAXPROCS2/GOMEMLIMIT512MiB; each actual package execution has a300-minute watchdog. Stages are sequential.

The pipeline runs compaction controls under race, exact six-leaf grant-bypass negative control, the bounded native R1/archive64 fixture, compaction1000 race, all853 normal pins and compaction100000 normal. Nonzero native or model stages keep the pipeline failed; subsequent model evidence is still retained. [Independent reviewer](review.py) checks matching loaded terminal supervisor exit0, all Git inputs and source stability, binary hashes/build modes, exact commands/exits, package/test terminals, full pin inventory, actual seed-body proofs and bounded native phase/boundary receipt. It cannot accept a launch or partial run.

No qualification result exists yet. Full original source-wide extended campaigns, actual100000 native cap, native matrices, scale/soak/lifecycle/import/admission/collector/rollout requirements remain open.

## Development race timeout retained

The earlier unfrozen compaction1000 race attempt closes nonzero at600.085s under the default10-minute Go package watchdog. It is failed development evidence, not a completed seed proof or a model defect finding. It is not restarted. The already-running frozen component qualifier uses an explicit300-minute watchdog and must still complete; do not infer acceptance from that configuration. The separate full158 race supervisor is live.

## Confirmed node-grant defect in this frozen source

The graph Walk callback flag is true for nodes. Compaction interprets it as payload and skips every relocated node grant check, instead redundantly checking payload grants. Sixteen new directed controls fail at this source by committing readable nodes whose ready/origin/destination/location grant has been revoked, across live/archive leaves/branches. The production correction is later and outside this immutable qualifier. Its execution may finish, but the independent review now reports execution evidence separately and `accepted=false` because the source has this confirmed defect. No existing successful test proves the omitted node-grant invariant. The qualifier remains running unchanged; its original source/commands/events are still useful evidence and cannot qualify the corrected code. [Counterexample evidence](../graph-compaction-node-grants-2026-10-10/original-node-grant-failures.log).

## Terminal review — 2026-10-10 17:12 UTC

The original supervisor finished with a matching loaded InvocationID, MainPID0
and actual exit0. Independent review verified all3407 Git inputs and unchanged
source, binary hashes/build modes, all ten exact commands/expected exits,
1000 race and100000 normal compaction seed bodies,853 saved pins, six deliberate
original-grant bypass failures and native R1/archive64 (two checkpoints, terminal
slot63, zero forbidden effects). [Review](qualification-review.json).

`execution_evidence_verified=true`, **`accepted=false`**: frozen source2de6dfa
contains the confirmed relocated-node grant validation defect. This verifies
execution of the old commands and does not qualify corrected grant safety or
latest-source publication. The100000 normal result is a simulation seed campaign,
not the failed actual100000-entry native gate. That native gate and all original
whole-plan requirements remain open.

Raw event files, before/after source manifests, executed driver, exact negative
overlay, compile logs and terminal supervisor receipt are retained in
[terminal execution archive](terminal-execution.tar.gz), with an independent
[artifact digest inventory](terminal-artifacts-sha256.json). The original external
root and binaries remain available to the existing reviewer. This job is closed;
the separate older158-family sharded race job remains live on source0404fc0.
