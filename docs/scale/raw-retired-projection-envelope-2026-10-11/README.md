# Strict retired terminal projection admission — 2026-10-11

Direct terminal Retire can remove the journal/input forests while leaving the
invocation source and terminal projection. The independent raw auditor now
strictly decodes its own complete outcome fields in this branch. It rejects
unknown, aliased, duplicate and escaped duplicate fields; scalar decoding checks
integer ranges and base64 result encoding. Generation/kind still must match the
retired cursor. Failed projections cannot carry results. External descriptors
must have a hash and exclude inline bytes; a hash without a pointer is invalid.
Limit metadata requires the failed limit error and mutually exclusive request
or entry fields. Nested limit-entry headers receive strict field admission.

Projection-only counters remain explicit. Lost forests do not provide an owned
result witness or rejected-operation prefix; descriptor validity does not prove
external result bytes, and rejected request/entry payload semantics are not
reconstructed. User result bytes remain opaque. No production projection or
outcome decoder is called.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphRetiredProjectionEnvelope|TestRawGraphRetiredSourceProjectionScope|TestRawGraphTerminalEnvelope|TestRawGraphCursorEnvelope|TestRawGraphStartEnvelope|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=4m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-retired-projection-native-20261011 go test -race ./worker -run '^TestNativeGraphContinuationFailedChildPromise$' -count=1 -v -timeout=6m
```

The first command exits0 in18.712s. All62 new physical JSON/protobuf controls
pass:16 positive and46 negative. Retired roots have empty forests. Every fixture
passes reference auditing before projection admission. Accepted cases require
RetiredProjectionOnly=1/Retired=1 and Journals=0/Terminal=0/Entries=0; no full
terminal-history acceptance is inferred. The existing retired source, live
terminal and cursor/start controls pass with four native R1/R3
indexed/unindexed checkpoint cases. ci-guard.py is the extracted new CI subset
executed against race.log; no hosted CI execution is claimed.

The native failed-child matrix covers R1/R3, archive=false/true and
buffered=false/true. Actual executable captures for both race commands, source
hashes prepared before either command, terminal verdict and closed native
storage inventory are retained alongside the logs.

The native command exits0 in187.311s with all eight failed-child cases,
eight retired-projection receipts,16 parent checkpoint frames and preserved
planned_child_failure errors. Both extracted CI guards pass. review.py verifies
416 unchanged source files and inventories all1900 closed native files
(61,667,284 bytes), retaining hashes/modes/nanosecond mtimes. actual-exits.json
records the observed command exit statuses. This is focused development evidence.

## Remaining scope

Strict projection admission does not prove orphan projection census, complete
retired result ownership/content or limit provenance, full parent/source-header
history, scalar presence/null semantics, all-peer/power durability or original
simulation/scale/fault/soak/rollout acceptance. Public continuation admission,
import and online collection remain disabled.
