# Independent cursor field presence — 2026-10-11

The raw journal auditor now requires the seven top-level cursor fields emitted
by every persisted cursor version: schema, invocation, base, count, epoch, kind,
and retired. Every present top-level field must be non-null, including optional
counters, start and checkpoint. Omitted optional fields and explicit numeric zero
or false remain valid. This check is independent of production cursor admission;
existing strict decoding handles unknown, duplicate and aliased fields first.

## Evidence

Baseline b9e34bf plus the new test, before the auditor change:

```sh
go test ./integrity -run '^TestRawGraphCursorFieldPresence$/^json$/^archive=false$/^retired=false$/^missing-retired$' -count=1 -v
```

Actual exit 1: the test found a cursor missing retired was accepted. Baseline log
is retained. Final command:

```sh
go test -race ./integrity -run '^(TestRawGraphCursorFieldPresence|TestRawGraphCursorState|TestRawGraphCursorVersionCapabilities|TestRawGraphCursorEnvelope|TestRawGraphStartEnvelope|TestRawGraphRetiredProjectionEnvelope|TestRawGraphRetiredSourceProjectionScope|TestRawGraphStartSourceHeaders|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=4m
```

Actual exit 0, package time 22.800s. All 208 physical snapshots pass independent
reference auditing before semantic admission: JSON/protobuf, legacy/archive,
live/retired, 26 controls each. There are 16 positives and 192 negatives. Controls
omit each required field, null each of all 17 fields, and accept valid metadata
and explicit optional zero/false. Existing wire/state/source/retired regressions
and all four native R1/R3 indexed/unindexed checkpoint flows pass.

421 source inputs were hashed before the final command and verified unchanged.
The actual live integrity executable SHA/build/command was captured. The CI test
selector includes the new test and its extracted receipt guard was executed
against the final race log; the workflow parses as YAML. Hosted execution is not
claimed. Native checkpoint stores were ephemeral and removed by their existing
test cleanup; this is no storage restoration or physical-power proof.

Only top-level cursor presence is covered. Nested start/request/outcome presence,
full retired history, lower-version physical compatibility, configuration
agreement and the original broad acceptance gates remain open. The four live
frozen campaigns predate this audit.
