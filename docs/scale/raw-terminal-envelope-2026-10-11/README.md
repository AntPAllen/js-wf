# Complete canonical terminal envelope admission — 2026-10-11

Own complete terminal wire fields now pass strict unambiguous decoding before
canonical generation/kind/result checks. Unknown, aliased or duplicate terminal
fields and typed limit_entry header fields are rejected. Existing terminal result
ownership/hash/byte checks and exact WF_STATE equality remain required.
Production wf.Outcome/replay decoders are not called; the shared JSON admission
primitive is used with own wire declarations.

User result bytes, rejected limit_request bytes and limit_entry.payload remain
opaque. This preserves wire compatibility without claiming to prove the rejected
operation against workflow execution. Limit-entry kind/payload semantics and
association with a specific limit failure remain separate open checks.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphTerminalEnvelope|TestRawGraphCanonicalEnvelope|TestRawGraphEveryCheckpointHistory|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=3m
```

Actual exit0, 18.222s. Ten physical positives cover both JSON/protobuf ordinary,
opaque result, failed, retained rejected request and retained rejected entry
outcomes. Twenty-two controls cover unknown/aliased/duplicate/escaped-duplicate
terminal fields and unknown/aliased/duplicate typed limit_entry header fields.
Every fixture first passes complete physical reference auditing, and its terminal
projection contains the same raw bytes, so matching WF_STATE cannot hide malformed
terminal metadata. Positive failed fixtures use an actual Failed cursor kind.

Canonical outer-envelope and historical checkpoint controls also pass. Four real
R1/R3 indexed/unindexed native checkpoint cases verify unpublished/published/
archived/terminal history with the tightened parser. Actual race executable,
source hashes, completed log and executed new CI subset are retained. This is
focused development, not frozen full qualification or a hosted CI result.

## Remaining scope

Rejected limit boundary semantics, general operation result equivalence, full
cursor/invocation/source schema, ambiguous promise completeness, clock/case
readiness, protected reader application replay, all-peer/storage proofs and
original full simulation/scale/fault/soak/rollout gates remain open. Both larger
frozen campaigns retain earlier sources. Public admission/import/collection
remain disabled.
