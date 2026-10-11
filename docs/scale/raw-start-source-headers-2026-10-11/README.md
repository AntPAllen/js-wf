# Owned invocation-source header admission — 2026-10-11

The independent raw auditor now checks the whole value list and canonical
spelling of each owned start field: graph-start token, input SHA256 and four
parent-return headers. Required fields must have exactly one matching value.
Absent parent fields and legacy Wf-Input-Ref must have no header at all, including
empty or aliased entries. Equal duplicates, conflicting second values, missing
required headers, empty lists and case aliases are rejected. This also avoids
Header.Get's panic on an empty value list. Exact pointer bytes remain required.
Unrelated transport headers are opaque and allowed; this is an owned-field
census, not a whitelist of every NATS header.

These checks use independent start declarations and direct raw header iteration,
without GraphStart.MatchesInvocation or production source admission calls.
Production MatchesInvocation still uses first-value reads; hardening that
production boundary remains separate work. No full parent causal history or
publisher/lease provenance can be inferred from these headers alone.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphStartSourceHeaders|TestRawGraphStartEnvelope|TestRawGraphCursorEnvelope|TestRawGraphRetiredProjectionEnvelope|TestRawGraphRetiredSourceProjectionScope|TestRawGraphSignalReservationAndBinding|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=4m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-start-source-headers-native-20261011 go test -race ./worker -run '^(TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=5m
```

The first command exits0 in28.215s. All232 new JSON/protobuf cases pass,
covering live/retired sources with/without complete parents:16 positive and216
negative. Each fixture first passes reference auditing. Invalid source headers
therefore fail beyond graph ownership/hash checks. Retired positive cases retain
projection-only counters, not full journal acceptance. Pointer-padding rejection
is an existing byte-equality regression. Cursor/start/projection/source controls
and four native R1/R3 indexed/unindexed checkpoints pass in the same command.

ci-guard.py is the exact new hosted-workflow subset extracted and executed
against race.log. Native children already run with raw auditing in the existing
workflow job; native-ci-guard.py checks the four focused cases and their raw
receipts locally. No hosted pipeline execution is claimed. Both actual live
race executables were captured. Source hashes were prepared before either run.
review.py verifies terminal logs, recorded actual exits, unchanged source inputs
and closed native files, retaining hashes/modes/nanosecond mtimes.

The native command exits0 in70.769s for four R1/R3 child and buffered-child
cases. Four raw receipts report two checked parent checkpoints each and one
retired child projection.417 source files remain unchanged. The closed native
file census and actual executable hashes are recorded in review.json. Both
focused guards pass. This is focused development evidence, not full source
qualification or physical-power durability.

## Remaining scope

Production source matching, full version-specific cursor semantics, unknown
transport/header interpretation, parent causal history, orphan projection/result
ownership, rejected-prefix reconstruction, scalar presence/null, all-peer/power
faults and original full simulation/scale/soak/rollout acceptance remain open.
The three larger campaigns continue under frozen inputs. Public continuation
admission/import/online collection remain disabled.
