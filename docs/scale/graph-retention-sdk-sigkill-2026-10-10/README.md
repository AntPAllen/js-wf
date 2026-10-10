# Canonical retention worker SIGKILL at SDK boundaries

This Linux opt-in test exercises the production worker and GraphHandler against
three native JetStream library servers in a fresh canonical-start/canonical-signal
graph namespace. A separate race-instrumented worker process stops at a confirmed
journal append and is killed and reaped with SIGKILL. The controller confirms the
old worker still owns its lease, then a fresh graph adapter and successor worker
consume the durable redelivery. Production LeaseTTL=12s and AckWait=13s are retained.

| Accepted append before kill | Target reused? | Required recovery |
| --- | --- | --- |
| Lookup StepRequested | No | Bind target and complete purge |
| Lookup StepCompleted | No | Replay original generation and complete purge |
| Purge StepRequested | No | Retry purge of original generation |
| Purge StepRequested | Yes | Fail stale; leave replacement and result unchanged |
| Purge StepCompleted | No | Replay accepted success |
| Purge StepCompleted | Yes | Replay accepted success; leave replacement unchanged |

Every case checks the exact accepted journal prefix, final canonical terminal and
result. Accepted lookup completions must contain the original invocation sequence.
Replacement cases compare retirement metadata and the replacement result after
recovery. Source and retention partitions are distinct so setup workers cannot
consume the retention operation accidentally. Child cleanup runs on failure.

The Tier1 corpus in ../graph-retention-workflow-replay-2026-10-10 cuts BEFORE append
commit; this test kills AFTER commit. Its last reuse case therefore replays a
recorded purge success, while the Tier1 cut before that completion retries the
old effect and must fail stale. These are complementary durable SDK states.

## Evidence

`initial-race.log` has actual exit 1: five cases pass, while the sixth uses the
incorrect expectation that a fully recorded purge success should fail stale.
`initial-source.json` and `initial-test.go.txt.gz` preserve that exact test source
and observed race binary. The runtime returned the proper completed outcome.
The final test corrects that expectation and verifies the recorded original
lookup generation. No production runtime changes or timeout increases were made.

`development-source.json` records hashes before the final six-case race run;
`development-binary.json` records the observed running executable, race build
info and actual arguments. `development-race.log` and its review record the final
actual process result. This is development-worktree evidence. Hosted CI requires
all six cases and receipts with no top-level skip; hosted execution is separate.

These tests cover the four healthy SDK append boundaries. Internal purge stages
already have separate native cut tests. Reader/active-lease waiting paths, combined
server/worker faults, independent full raw canonical invariant auditing, original
10k/10k/1k concurrency and full current-source acceptance remain open. Native
server stores are temporary fixtures; this is worker-process crash recovery,
not VM power-loss or disk-failure qualification. Online collection and admission
remain disabled.

## Operator CI admission correction

GitHub run 38095730088 at 24dc81d failed with no jobs and no available job log.
The workflow used runner.temp in job-level env, where runner is unavailable.
The root now appears in the execution and capture steps' env instead, preserving
the same path for both. This corrects a known invalid context use; the service
did not expose an annotation identifying its rejection cause.
[GitHub's context availability table](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#context-availability)
permits runner in step env and excludes it from job env. YAML and every shell
step parse; the new native job's six-case gate accepts the real passing log.
Hosted admission and actual job execution require their own observations.

After pushing df1f3f5, GitHub admitted operator workflow run 38096239213
and created all 13 jobs, including canonical-postgres-cli. The queued job list
is retained in operator-ci-after.json. This verifies workflow admission after
the context correction; job execution and test results remain pending.
