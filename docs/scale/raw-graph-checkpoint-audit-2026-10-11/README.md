# Raw checkpoint pointer/frame binding audit — 2026-10-11

The raw canonical journal audit independently binds each published checkpoint
pointer to the declared request and completion in its logical journal. It checks
schema eligibility, invocation, synthetic sequence, index/epoch bounds, request
position, canonical object/hash shape and cumulative SDK step position. The
completion must name the same result and carry an owned frame edge. Hashed frame
bytes must match version, workflow identity, generation, anchor, stage, SDK
position and the request's exact locals hash. Archive/live traversal preserves
the full logical prefix. No production cursor validation or checkpoint reader
is called. Shared frame types and unambiguous JSON decoding define wire format;
production checkpoint.Decode/admission are not used.

## Evidence

The completed focused development command was:

```sh
go test -race ./integrity \
  -run '^(TestRawGraphReferencesAndIndependentCorruptionControls|TestNativeRawGraphReferenceAuditQuiescentReaders|TestRawGraphJournalGenerationsOutcomesAndArchive|TestRawGraphCheckpointPointerAndFrameBindings|TestNativeRawGraphCheckpointAudit)$' \
  -count=1 -v -timeout=5m
```

Actual exit0, package20.900s. Source hashes were captured before execution and
rechecked unchanged afterward. This is development-worktree evidence, not
complete clean-source qualification. The executable was not captured: a live
receipt attempt matched a shell and failed; no binary-binding claim is made.

Fourteen raw checkpoint controls reject pointer generation/sequence/request/
position/object, completion result, request kind, unowned frame, and frame
generation/anchor/stage/position/version/locals. Each fixture independently
passes physical reference auditing before semantic rejection. The four existing
JSON/protobuf whole/archive positives now contain a real pointer/frame for the
archive cases, replacing the earlier deliberately opaque placeholder.

Four native R1/R3 × ordinary/indexed layouts use production Start, Append,
checkpoint.Encode, PublishCheckpoint and CompactCheckpoint to construct state.
The independent audit accepts all four states: unpublished, published, archived,
and terminal with projection. Sixteen RAW_CHECKPOINT_AUDIT receipts prove these
calls ran. Existing reference and journal corruption tests also pass. The new CI
guard requires all fourteen controls, all layouts and all sixteen receipts; it
was executed against the actual log. Hosted execution remains separate.

The first normal native fixture attempt failed both R3 cases during provisioning
before an audit; both R1 layouts passed. Its summarized actual terminal output is
preserved in initial-provisioning-failure.json. A leader/peer readiness guard was
added before provisioning, matching the existing native reference fixture.
The corrected race suite passed all four layouts.

## Scope still open

Materialized frame state, promise outcomes, signal/timer history, latest-pointer
discovery across all completed checkpoints, protected-reader semantic replay,
signal binding/index contents, orphan projections, lease/client histories and
full native scale/physical persistence remain separate. No runtime change,
whole-phase completion or admission/online-collection enablement is claimed.
The frozen complete159 job remains source00eeb13; it predates this addition and
must finish under its original invocation. It is not restarted by this change.
