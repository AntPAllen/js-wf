# Independent canonical signal wire admission — 2026-10-11

The raw auditor now owns complete declarations for canonical signal requests,
reservations and bindings. It no longer decodes these records into production
journal descriptor types. Strict unambiguous decoding rejects unknown, aliased
and duplicate fields at all nested levels. Separate checks require every emitted
field and reject null, including zero-valued indexes/input_size and index packets.
Request fingerprints use the independently declared canonical request bytes.
Existing source ordering, generation, identity, body grants/hash/size, reservation
binding equality, consumed-event census and independent index checks remain.

## Evidence

Baseline93055c2 plus the new fixture/test:

```sh
go test ./integrity -run '^TestRawGraphSignalEnvelope$/^json$/^signal-input$/^root$/^unknown$' -count=1 -v
```

Actual exit1: the audit accepted an unknown field in a physical signal reservation.
Final broad command:

```sh
python3 docs/scale/raw-signal-envelope-2026-10-11/run.py
```

Actual exit0 in33.832s for all35 TestRawGraph top-level tests and four R1/R3
indexed/unindexed native checkpoint flows (including orphan/tombstone checks).
190 new physical controls span JSON/protobuf, input/queue records and root,
binding, input and request levels:28 positive/162 negative. Valid/whitespace,
unknown/alias/equal-duplicate and missing/null for every declared field are
checked. Each fixture passes reference auditing before semantic admission.

An initial compile failure after removing the production descriptors left an
unused journal import. Its exact source, pre-run hashes, command exit1 and log
are retained in compile-failure/. It did not execute a test binary. The baseline
acceptance failure remains separate.

Additional native validation:

```sh
python3 docs/scale/raw-signal-envelope-2026-10-11/native-run.py
```

That command selects buffered signals and selection-priority SDK flows with
WF_GRAPH_SDK_RAW_AUDIT=1. native-command.json/native-executable.json/native.log
contain actual exit, executable and receipts; review.json records final timing
and scope.426 source inputs were hashed before the broad race and native commands.
The new exact190-case CI receipt guard was executed locally and the workflow
parses as YAML; hosted CI is not claimed. Native stores are ephemeral and removed
by existing test cleanup, so no restoration or physical-power proof is claimed.

Consumed-signal envelopes, original WF_SIG transport provenance after removal,
full retired/parent/lease causal history, concurrent client linearizability and
original full native/fault/scale/soak/rollout gates remain open. Four live frozen
campaigns predate this audit. The deterministic suite remains161 families/894
pins; no full current-suite acceptance is claimed here.
