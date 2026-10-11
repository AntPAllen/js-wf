# Complete isolated projection census — 2026-10-11

GraphJournalSnapshot now requires a complete Projections map in addition to its
source census and projection reader. Every key must be a valid type.id. Required
terminal reads must exist in that census and match its captured bytes. Every
captured projection must be consumed by a current live terminal or a retired
canonical cursor with matching generation/kind/descriptor. An orphan or projection
attached to a nonterminal journal is rejected. A surviving retired projection is
also audited after source deletion; RetiredProjectionOnly remains explicit.

The native collector enumerates every live WF_STATE key/value before graph
traversal, rejecting duplicate keys or failed reads. Deletes are excluded from the
logical KV census. Existing pre/post stream first/last sequence and message-count
checks guard source/projection changes. The isolated namespace and full quiescence
contract remain required. Physical KV tombstone reclamation is a separate gate.

## Evidence

Baseline ca3f98b plus the new orphan test:

```sh
go test ./integrity -run '^TestRawGraphOrphanProjectionBaseline$' -count=1 -v
```

Actual exit1: orphan.id was ignored and the test failed. Final broad command:

```sh
python3 docs/scale/raw-projection-census-2026-10-11/run.py
```

Exact Go command and actual exits are in command.json. The selection includes all
TestRawGraph tests and the four native checkpoint tests. New physical cases cover
JSON/protobuf × legacy/archive ×12 controls: valid, orphan, malformed key, missing
census, changed/missing reader, missing terminal, empty running journal, projection
on a running journal, surviving retired projection, wrong surviving generation,
and cleared retired state. There are48 controls (16 positive/32 negative) plus the
original orphan regression. Each passes reference auditing before semantic checks.
All four native R1/R3 indexed/unindexed flows inject a real orphan KV value and
require rejection, then delete it and require successful audit with its logical
tombstone excluded. Exact receipt guards were executed locally; hosted CI is not
claimed. The final race suite passes34 raw graph top-level tests in34.422s.

The first broad run exited1 in29.133s. Two existing SignalConsumed fixtures still
carried the terminal projection from the Completed entry they replaced. Two new
archive empty-running fixtures accidentally cleared their required input forest
through a shared slice. Those fixtures were corrected. Failed logs, both test
sources, pre-run hashes, actual executable and command exits remain in
fixture-development-failure/. Auditor checks were retained.

Additional native child/buffered-child race runs exercise retained/retired child
projections through the new census. Their actual commands/executable/receipts are
in native-command.json, native-executable.json and native.log. Read review.json
for final runtime and exact outcome.424 source inputs were bound before both
commands. Native test stores are ephemeral and removed by existing cleanup; no
full storage restoration or power proof is claimed.

This closes logical orphan/current-generation projection admission for a complete
isolated snapshot. It does not prove public online namespace completeness, full
retired causal history, concurrent publisher linearizability, physical tombstone
GC, all-peer/power durability or original broad acceptance. Four frozen long
campaigns remain active and predate these changes.
