# Same-store sustained mutation harness: local smoke

These are **35-second smoke runs**, not accepted ten-minute release pairs.
The worktree was based on `85390ab`. The archive retains the Go fixtures,
overlays, original JSON events, original matrix artifacts, runner reports,
independent checks and raw receipt proofs. Every archived member was read back
and verified against its original bytes and SHA256 manifest.

| Pair | Original completed invocations, baseline / mutant | Result |
| --- | --- | --- |
| CAS | 168 / 196 | Baseline passes; mutant acknowledges two distinct physical entries at logical index2/result42 and the raw-state checker rejects it |
| Start repair | 196 / 196 | Baseline repairs the generation-tagged dispatch and completes all29 guard invocations; mutant leaves the durable29th orphan without dispatch, journal or terminal |

Each original row executes the six workload classes, one scheduled real
journal-leader kill, history checks, retained audit, measured terminal/progress
p99 and physical run-queue drain. The guard challenge follows on the same R3
process cluster and retained stores with an additional verified SIGKILL. Original
cohort high-water audits still pass after both actual semantic failures. No
records are erased or repaired to pass them. The original cohort audits prove
valid state and unchanged counts, not every record's byte identity.

The CAS baseline also passes under race in91.57s test /92.591s package. Its
original matrix artifacts and actual named pass are retained. `go vet -p=1
./integration` passes. Four Python evidence tests reject missing/shortened runs,
incorrect seed/mode/cohort, missing preservation/workloads/faults/artifacts,
over-budget measurements, malformed raw times and truncated or duplicated
terminal cohorts. The two mandatory actual compile-error/unrelated-failure
controls are rejected for each local runner pair.

The final runner independently rechecks all five original phases, recomputing
per-class p99 from each original latency sample and its exact enabling/observed
nanoseconds. Go fixture bytes match the invocation reports' recorded hashes.
The runner was tightened after those invocations began; the archive includes
the final independently exercised validator, while original invocation reports
retain their original runner inventory hash. `independent-check.json` contains
the regenerated checks, and `raw-proof.json` independently decodes the CAS and
start-generation receipts.

Commands:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 python3 scripts/check-invariant-mutations.py \
  --mixed-cas --sustained 35s --output /tmp/js-wf-sustained-cas-smoke-20261002
GOMEMLIMIT=512MiB GOMAXPROCS=2 python3 scripts/check-invariant-mutations.py \
  --mixed-start-repair --sustained 35s --output /tmp/js-wf-sustained-start-smoke-20261002
GOMEMLIMIT=512MiB GOMAXPROCS=2 WF_MATRIX_CHAOS=1 WF_MATRIX_DURATION=35s \
  WF_SUSTAINED_MUTATION=cas FAULT_SEED=42 \
  MATRIX_ARTIFACT_PREFIX=/tmp/js-wf-sustained-cas-race-20261002 \
  go test -race -p=1 -json ./integration \
  -run '^TestSustainedMixedMutationAfterJournalLeaderKills$' -count=1 -timeout=10m
python3 -m unittest discover -s scripts -p test_sustained_mutation.py
```

Hosted all-six smoke and then all-six actual ten-minute pairs remain required.
The independent 200-seed full matrix, full timer-cut combinations and24-hour
full-matrix soak remain open.
