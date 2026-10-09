# Canonical continuation stage execution component — 2026-10-09

The worker execution path now reads checkpoint frames from its pinned canonical
graph rather than a legacy manifest. It restores the existing SDK checkpoint
context and dispatches the registered stage. Subsequent appends maintain the
anchored suffix and absolute journal index while the graph delivery retains its
full verified payload-reference map. No legacy checkpoint/journal read is used
when a graph has no completed boundary.

**Public continuation admission remains closed.** Internal migration tests attach
stage registrations after ordinary graph worker construction and invoke delivery
execution directly. This does not establish a deployed continuation workflow or
compact graph history. Archive/prefix compaction, bounded resume discovery,
materialized payload ownership after prefix collection, full audit/offline replay
and process-kill/limit qualification remain required.

## Executed development evidence

`final-race.log` runs:

```sh
go test -race ./worker -run '^(TestNativeGraphContinuationHandoff|TestContinuationPreservesGlobalJournalLimitAndTerminalSlot|TestContinuationAnchorUsesHistoricalRuntimeFacts)$' -count=1 -v
```

The native fixture now covers R1 and R3 with a JetStream domain. It retains the
previous handoff checks (invalid checkpoint/lost lease cannot dispatch; retry
does not duplicate suspension and survives dedup). It additionally resumes an
owned initial checkpoint, reads materialized state/locals/input, records one
large RunOnce outcome, saves large state, checkpoints into a second named stage,
restores that state and completes a large terminal result. A duplicate terminal
delivery must not reenter either stage or its effect. The initial handler must
not run after the initial checkpoint. Both option orders continue to reject
normal construction with continuation registration. Legacy WF_JRN remains empty.

The initial checkpoint is prepared by the fixture. This is direct execution of
the production delivery function with test-only registration; no autonomous
worker dispatch loop or collector compacts the prefix. The effect/state/frame
assertions establish this component's behavior, not full continuation admission.
Existing legacy continuation global-limit/terminal-slot and historical-anchor
controls also pass.

`initial-race.log` retains the fixture compilation error from using nonexistent
`Client.Result`; it now uses `Await`. `stages-race.log` is the first R1 pass before
adding R3 domain and option-order admission checks. Sources are observed at
review, not frozen before compilation. Full/current/extended simulation and all
original broader runtime/native/scale/actual24h/drain/adoption/release gates stay
open. The live frozen full race at `9a1ccdc` excludes this code.
