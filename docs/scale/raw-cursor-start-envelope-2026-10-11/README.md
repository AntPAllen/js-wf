# Independent cursor and canonical start admission — 2026-10-11

The raw journal auditor strictly decodes its own cursor and canonical start wire
fields using the shared unambiguous JSON primitive. The start descriptor is
checked both in the cursor and in the owned input forest. Production graph
readers/start admission do not validate their own output. Unknown, aliased,
duplicate and escaped duplicate field names are rejected at these boundaries;
integer type/range checks apply. Existing canonical identity, source pointer,
input hash/length, owned object and complete physical reference checks remain.

Parent-return metadata must either be wholly absent or contain a valid parent
workflow identity, nonzero invocation and valid signal token. This does not
require the parent to remain in the audited cohort: it may already be retired.

## Focused evidence

```sh
go test -race ./integrity -run '^(TestRawGraphCursorEnvelope|TestRawGraphStartEnvelope|TestRawGraphEveryCheckpointHistory|TestRawGraphJournalGenerationsOutcomesAndArchive|TestRawGraphSignalReservationAndBinding|TestRawGraphRetiredSourceProjectionScope|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=4m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-cursor-start-native-20261011 go test -race ./worker -run '^(TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=4m
```

The first command exits0 in21.368s. There are74 new physical cases:
12 positive and62 negative, covering JSON/protobuf journals, live/archived
cursor controls, nested cursor starts, separately owned start descriptors and
self-consistent valid/invalid parent fields. Every case first passes complete
physical reference auditing. Negative cases therefore demonstrate semantic
rejection beyond object hashes and reference census. Historical checkpoint,
source reservation, retired projection scope and four native R1/R3
indexed/unindexed checkpoint regressions pass in the same command.

The exact new CI guard was extracted into ci-guard.py and executed against
race.log, requiring all74 case receipts. The hosted pipeline was not run here.
Source hashes were prepared before the race run. Its executable capture was
missed; worker-executable.json records the actual live native race executable.
The independent native run, verdict and closed storage inventory are recorded
in native.log and review.json.

## Remaining scope

This is focused development evidence. Full cursor version-specific semantics,
required scalar presence/null rules, duplicate/extra source headers, complete
parent causal history and strict retired projection wire admission remain open.
Checkpoint RawMessage admission remains governed by the existing live checkpoint
checks. Retired projections are still reported separately from full journals.
This does not establish original full simulation, scale, fault, power durability,
soak or rollout acceptance. Public continuation admission/import/online
collection remain disabled. The larger campaigns retain their frozen sources.
