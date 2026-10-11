# Independent terminal outcome presence — 2026-10-11

Canonical outcomes require inv_seq, and every present top-level outcome field
must be non-null. A rejected limit_entry requires both non-null kind and payload.
The check applies before advancing a live terminal's journal prefix and before
admitting a retired projection. Existing strict decoding, generation/kind checks,
result descriptor validation and live rejected-operation prefix checks remain.
Optional omission, empty result/error and opaque user result bytes stay valid.
RetiredProjectionOnly still describes the limited surviving witness.

## Evidence

Baseline 3bab6c4 plus the new fixture:

```sh
go test ./integrity -run '^TestRawGraphOutcomeFieldPresence$/^json$/^retired=false$/^null-error$' -count=1 -v
```

Actual exit1: the audit accepted error:null as an empty error. Final command:

```sh
python3 docs/scale/raw-outcome-presence-2026-10-11/run.py
```

The runner records its exact Go command and actual child exit0. Final race package
runtime23.234s.76 physical JSON/protobuf × live/retired cases (28 positive/48
negative) pass reference auditing before semantic admission. Controls cover
missing/null generation, null in each optional outcome field, missing/null limit
entry headers, valid omission/whitespace/empty scalars and valid failed/rejected
outcomes. Cursor/start/source/projection envelope/state tests, rejected-limit
prefix/continuation tests and four R1/R3 indexed/unindexed native checkpoint flows
also pass.423 source inputs hashed before the final run remained unchanged. The
actual test executable SHA/build/command was captured. The exact new CI guard
passed locally; workflow YAML parses. Hosted CI is not claimed.

The first race run exited1 in21.278s due to one positive JSON whitespace fixture.
JSON journal entry serialization compacted its RawMessage payload, while the
fixture projection retained whitespace. The auditor correctly rejected unequal
terminal bytes. The fixture now binds its projection to actual compact JSON
bytes; protobuf preserves whitespace. That failed log, exact test source,
pre-run source hashes, executable and command exits are retained in
fixture-whitespace-failure/. The auditor was not relaxed to admit unequal bytes.

Native stores were ephemeral and removed by test cleanup: this is no restoration
or physical-power proof. Full retired causal history, rejected-operation payload
provenance after retirement, other nested field-presence schemas, configuration
agreement and original broad acceptance gates remain open. Four long frozen
campaigns remain active and predate these checks.
