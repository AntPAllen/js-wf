# Independent canonical start/request presence — 2026-10-11

The auditor requires schema, token, request, input_sha256 and input_size in each
canonical start descriptor. Its request requires type/id. Every present start
and request field must be non-null; omitted optional parent fields and explicit
parent_invocation=0 remain valid. Existing independent wire, identity, parent,
source and physical input-byte checks still apply. Both cursor metadata (including
retired cursors) and owned input-leaf descriptors receive this presence check.
Production admission helpers are not called.

## Evidence

Baseline af40da0 plus the new test/fixture mutation:

```sh
go test ./integrity -run '^TestRawGraphStartFieldPresence$/^json$/^cursor-retired$/^missing-input_size$' -count=1 -v
```

Actual exit1: missing input_size was accepted in a retired cursor. Final run:

```sh
python3 docs/scale/raw-start-presence-2026-10-11/run.py
```

The runner preserves the exact Go command, actual child exit0 and timestamps in
command.json; its race log reports22.862s. All126 physical snapshots pass reference
auditing before semantic checks: JSON/protobuf × cursor-live/cursor-retired/
owned-input ×21 controls.18 positive/108 negative cases cover missing/null required
start and request fields, null optional parent fields, valid metadata, whitespace
and explicit optional zero. Existing cursor/state/envelope/source/retired controls
and four R1/R3 indexed/unindexed native checkpoint flows pass too.

422 source inputs hashed before the command remained unchanged. The actual live
integrity executable SHA/build/command was captured. The new workflow selector
includes this test; the extracted exact receipt guard was executed on race.log,
and the workflow parses as YAML. Hosted CI is not claimed. Native checkpoint
stores were ephemeral and removed by existing test cleanup; no storage restoration
or physical-power proof is claimed.

Only start/request presence is closed here. Outcome/other nested envelope presence,
full retired causal history, configuration agreement, all-peer/power durability
and original broad acceptance gates remain open. Four live frozen campaigns
predate this audit. RetiredProjectionOnly still records its limited evidence.
