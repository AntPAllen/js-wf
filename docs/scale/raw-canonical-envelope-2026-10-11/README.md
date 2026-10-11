# Strict canonical journal envelope admission — 2026-10-11

Raw graph journal entries now decode their complete four-field canonical envelope
with the unambiguous JSON primitive and own wire declaration. Unknown fields,
case aliases, duplicate and escaped-duplicate fields are rejected before owned
entry loading. Existing schema, generation, exact sequence and owned entry-hash
checks remain required. The production journal cursor/entry envelope reader is
not invoked. JSON/protobuf inner entry codecs remain shared wire definitions.

## Actual evidence

Race command:

```sh
go test -race ./integrity -run '^(TestRawGraphCanonicalEnvelope|TestRawGraphSDKStepEnvelope|TestRawGraphEveryCheckpointHistory|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=3m
```

Actual exit0, 17.174s. Four positive physical fixtures and fourteen
corruptions cover both JSON/protobuf inner entries. All fixtures first pass the
complete independent physical reference audit. Corruptions include outer unknown,
aliased, duplicate/escaped duplicate fields, generation, sequence and entry hash.
The whitespace positive starts with padded source JSON; fixture node marshaling
may compact it, so this is compatibility rather than exact whitespace retention.
Existing SDK envelope and every-checkpoint physical cases also pass. Four native
R1/R3 indexed/unindexed cases verify unpublished, published and archived frames.

Source hashes were checked unchanged, and the new CI subset guard was executed
against race.log. The executable exited before a live binary receipt was captured;
this is focused development evidence and not frozen qualification. No retry or
runtime workaround was needed.

## Remaining scope

Full runtime cursor, invocation/terminal/source schema, operation-specific result
semantics, clock/case readiness, all-peer storage, full current-source simulation
and original scale/fault/soak/rollout gates remain open. Both live larger campaigns
retain earlier sources. Public admission/import/collection remain disabled.
