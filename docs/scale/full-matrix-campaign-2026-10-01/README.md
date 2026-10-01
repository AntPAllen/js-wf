# Whole sustained Tier 2 matrix campaign

The existing tier2-matrix-leaders workflow adds row=all. It selects all thirteen
sustained fault variants: journal/consumer leader kills, all-server kills,
majority-side partition, random worker kill, active worker pause, asymmetric
reply isolation, worker clock, both server clock offsets, fan-out restart,
block-device stall and rolling upgrade. Each test still drives the full parent
mix plus children/grandchildren, fault verification, retained/history audits,
progress/terminal p99, queue drain and five-minute recovery checks.

All-row campaigns group up to twelve consecutive seeds per job. Two hundred
seeds per row expand to 2,600 executions in 221 jobs, within the hosted 256-job
matrix limit. Groups run sequentially and stop on the first failed seed; missing
later seeds therefore cannot count clean. Jobs continue independently with
fail-fast=false and maximum parallelism four. Per-seed Go timeouts stay 18/20
minutes; grouped jobs have five hours for at most four hours of test attempts
plus setup/artifacts. Individual-row campaigns retain one seed per job, their
25-minute bound, original job names and artifact names.

Go JSON events are retained per row/seed. A result guard requires exactly one
pass for the expected test, package completion and the requested 35/600-second
minimum elapsed time. Skip, no tests, wrong test, duplicate result, failure,
missing completion and shortened duration are rejected. Human Go output is
rendered for compatibility with the existing one-row campaign verifier.
Artifacts preserve each executed seed's raw fault/state/latency history files;
all-row artifact names include their seed range. No runtime/checker or latency
gate is weakened.

Twelve planner/result/legacy verifier controls pass. They establish exact
per-row seed coverage, job limits and duration separation. A real journal-leader
35-second smoke on seed 1 passes in 46.818 seconds; its JSON result is accepted
as smoke and explicitly rejected as ten-minute release evidence. This is a
runner diagnostic, not a sustained matrix pass. Raw event/human logs and plan
are retained. Hosted one-seed-per-row ten-minute execution is pending.

    gh workflow run tier2-matrix-journal.yml --ref main -f row=all -f seeds=1 -f duration=10m
    gh workflow run tier2-matrix-journal.yml --ref main -f row=all -f seeds=200 -f duration=10m

The 200-seed command is the full Tier 2 campaign; creating its job plan does
not prove completion. Every planned row/seed must execute successfully at the
recorded revision. A one-seed campaign or 35-second run cannot clear that gate.
This is still the three-node tier. The five-node 24-hour full-matrix soak,
written explanations for fencing/re-enqueues, six-mutation mixed chaos release
check and remaining capacity/runtime requirements remain independent.

## Terminal whole-matrix verifier

scripts/check-full-matrix.py reads terminal Actions job metadata and the
corresponding full job-log ZIP. It requires exactly the planned setup/groups,
all completed successfully, the same full checkout SHA in every group log,
all thirteen rows, each consecutive seed exactly once, ten-minute execution,
the per-seed JSON result guard, retained audit/workload counts and all workload
terminal/progress p99 markers. It reuses the existing one-row semantic checker.
A skipped, live, missing, duplicate, failed, shortened or wrong-revision result
fails closed. Group logs are split at their explicit row/seed execution markers;
missing later seeds cannot disappear into aggregate green metadata.

    gh run view RUN_ID --json status,conclusion,headSha,jobs > jobs.json
    gh api --allow-escape-sequences repos/AntPAllen/js-wf/actions/runs/RUN_ID/logs > logs.zip
    python3 scripts/check-full-matrix.py --jobs jobs.json --logs logs.zip --seeds 1 --output full-matrix-report.json

For the release campaign use --seeds 200. One- and twenty-seed full-matrix
results explicitly report clears_tier2_200_seed_gate=false; even a successful
200-seed campaign reports clears_tier3_24_hour_soak=false. The verifier checks
recorded runtime assertions; it does not independently reconstruct raw stores
or prove unrelated release requirements.

Five additional controls cover all supported counts/scopes, job and source
identity, partial/duplicate/wrong/shortened seed logs, semantic p99 failures,
registry consistency and duplicate ZIP job logs. Seventeen matrix tests and
all twenty-five script tests pass. Fixtures in those controls are synthetic,
not release evidence. Actual live run 36845868029 metadata is retained and
rejected as unfinished. Its one-seed-per-row ten-minute campaign remains live;
final verification is pending. Production/model sources are unchanged.
