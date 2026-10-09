# Canonical continuation recovery component

## Behavior

A v5 `graph-continuation` scanner discovers published checkpoint pointers through a captured canonical Start catalog watermark. It reads lifecycle metadata and the native invocation binding, defers repair while a delivery lease exists, and publishes fresh wakeups after rechecking token, invocation, checkpoint sequence and eligibility. It acquires no reader pins and opens no input/frame payloads. Workers independently validate those owned payloads before stage execution.

The scanner covers a crash after checkpoint pointer publication, including before suspension, and a lost/consumed wakeup at the checkpoint boundary. Later timer/signal suspension goes through its own wait recovery. Terminal, retired, purging and pending generations are ineligible. Unknown reads/publications preserve the confirmed cursor prefix; a changed resume condition is skipped only after a fresh canonical witness. The existing fenced leader/cursor loop supports this kind; the worker loop builder adds it only for explicit v5 library configuration.

## Executed component evidence

- `final-metadata-race.log`: 15 seeded transport cases cover boundary and pre-suspension dispatch, held/uncertain leases, forged native source, precommit/lost-ack publication, confirmed-prefix retry, authority uncertainty, later waits, terminal/retired/purging states, and wait changes before and during source validation. Fresh retries inside the dedup window dispatch again. Forbidden payload reads and root CAS attempts are counted and must remain zero in every case. Dry run never dispatches. Missing/v4 index configuration is rejected. Existing captured catalog cycle checks run all four scanners at 16 seeds with one-entry budgets.
- `native-loop-race.log`: native R1 and R3/domain component tests purge all handoff wakeups twice and require fresh scanner dispatch each time, then purge again and require the production fenced repair loop to dispatch with exact invocation/checkpoint identity and joined shutdown. Held lease defers; terminal is skipped. Existing internal two-stage state/result/effect/duplicate controls pass with no legacy journal writes. Public constructor continuation admission remains rejected in both option orders.
- `legacy-repair-race.log`: existing graph reconciliation, canonical Start, anchored legacy context and CLI admission controls pass.
- `bound-start-limit-race.log`: exact native binding/generation and dedup recovery controls for the shared client helper pass, alongside the legacy global journal limit and terminal slot control.

`initial-race.log`, `final-race.log`, and `final-model-race.log` retain preceding passing component runs. `initial-prefix-retry-assertion.log` retains a fixture failure: its one-entry retry budget incorrectly assumed a refreshed catalog could not rediscover the earlier noncandidate quorum witness. The corrected retry consumes that bounded entry too and requires the candidate to dispatch; cursor-prefix checks are preserved.

`executed-review.py` checks required terminal passes and source hashes observed at review. These are development component controls, not a frozen full/extended simulation campaign. The previously qualified normal/race suites at `9a1ccdc` exclude this code.

## Remaining scope

This scanner requires an already published v5 pointer. Autonomous discovery before the first pointer publication is not proven; normal retained delivery redelivery can still resume that cut. The initial checkpoint in the native fixture is prepared explicitly and stage execution invokes the production delivery method internally. Full initial SDK flow, OS process-kill integration and complete autonomous worker scheduling remain unqualified.

Archival prefix compaction, bounded worker reference loading, collection-safe materialization, full audit/offline/import integration, v5 CLI/deployment and public continuation admission remain incomplete. Every original full/current/extended/runtime/native/scale/actual24h/physical-drain/adoption/release requirement remains required.
