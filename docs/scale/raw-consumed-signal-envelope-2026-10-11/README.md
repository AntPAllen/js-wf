# Independent consumed-signal wire admission — 2026-10-11

Consumed SignalConsumed payloads now receive strict unambiguous decoding into the
existing independent signal declaration before journal prefix advancement. This
rejects unknown, aliased and duplicate fields, including escaped duplicate keys.
sig_seq/name are required. Every present event field must be non-null. Present
canonical markers require index/token (index0 still valid), and present child
metadata requires type/id/inv_seq plus non-null optional metadata. Payload/result
bytes remain opaque; binding and causal-history checks stay separate.

Rejected limit-entry SignalConsumed payloads use the same wire check before their
existing sequence/prefix validation. Canonical markers remain required by the
existing canonical binding census; legacy signal events may omit the marker.

## Evidence

Baseline a9006aa plus the new physical fixture/test:

```sh
go test ./integrity -run '^TestRawGraphConsumedSignalEnvelope$/^json$/^root$/^unknown$' -count=1 -v
```

Actual exit1: unknown fields were accepted in a physical consumed-signal event.
Final broad command:

```sh
python3 docs/scale/raw-consumed-signal-envelope-2026-10-11/run.py
```

Actual exit0 in36.729s for all36 TestRawGraph top-level tests, including rejected
limit boundaries/prefixes/continuations and four native checkpoint/orphan/delete
flows.50 new physical controls (8 positive/42 negative) span JSON/protobuf and
root/canonical-marker levels. They exercise valid/whitespace, unknown/alias/equal
and escaped duplicates, missing required fields, null in every root field and
null/missing marker index/token. Every snapshot passes reference auditing before
semantic admission. Child nested shape checks use the existing independent typed
declaration; exhaustive child-only corruption controls remain a separate gate.

An initial compile failure left an unused encoding/json import. Its exact source,
pre-run source hashes, exit1 and log are preserved in compile-failure/; no test
binary ran there. The actual semantic baseline failure is separately retained.

Additional native command:

```sh
python3 docs/scale/raw-consumed-signal-envelope-2026-10-11/native-run.py
```

This selects R1/R3 child/buffered-child flows with WF_GRAPH_SDK_RAW_AUDIT=1. Exact
command/executable/exit and final receipts are preserved in native-command.json,
native-executable.json and native.log; review.json reports final scope/timing.
428 source inputs were hashed before the broad/native commands. The extracted
50-case CI guard passed locally; YAML parses. Hosted CI is not claimed. Native
stores are ephemeral and cleaned by tests, so no restoration/power proof is made.

Full child/parent/lease causal history, consumed-body equivalence across retired
history, original transport provenance, concurrent client linearizability and
original broad acceptance gates remain open. Four frozen long campaigns predate
this audit. Full current161-family/894-pin qualification is still outstanding.
