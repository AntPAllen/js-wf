# Rejected limit operations against retained history — 2026-10-11

The independent raw journal auditor checks retained rejected operations before
advancing the terminal record, while the request/completion prefix is available.
Limit metadata requires Failed, the exact journal limit error, no result and
exactly one rejected operation. A rejected request must have an unambiguous SDK
request envelope, a kind and no outstanding prior request. Rejected attempts
require the next per-invocation count and nonempty panic error; rejected signals
require an advancing sequence and nonempty name. A rejected suspension requires
nonempty waiting_on and either an outstanding request or a successful preceding
checkpoint completion matching the continuation stage. Typed rejected payloads
reject unknown fields, aliases and duplicates. Unsupported rejected entry kinds
are rejected. None of these rejected operations advances the prefix state.

Own wire declarations and the shared JSON admission primitive are used. Production
workflow replay, stepwire decoders and handler execution are not used. Successful
terminal metadata carrying a rejected operation is rejected as well.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphRejectedLimitBoundary|TestRawGraphRejectedLimitPrefix|TestRawGraphRejectedLimitContinuation|TestRawGraphTerminalEnvelope|TestRawGraphCanonicalEnvelope|TestRawGraphEveryCheckpointHistory|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=3m
```

Final actual exit0 in19.025s. Eight positive and40 negative physical JSON/protobuf
fixtures first pass complete graph reference auditing, with hashes, owned edges,
complete object census and matching terminal WF_STATE bytes. Six component
controls cover nonzero prior attempt/signal positions and four cover completed
checkpoint continuation admission. Existing terminal, outer-envelope and every
checkpoint history controls pass. Four native R1/R3 indexed/unindexed checkpoint
cases pass through publication, archive and terminal. These native cases do not
exhaust capacity. The actual live executable hash/build receipt and unchanged
final source hashes are retained. The new CI assertions were extracted and
executed against this final log; this is not a hosted CI result.

The initial combined run passed in17.857s before adding the four continuation
controls. Its source hashes/log/executable receipt are retained separately.
The standalone continuation run passed in1.020s; the final combined run includes
all four controls and uses the final source manifest.

## Remaining scope

Configured capacity is not durable in these records; this checker cannot prove
that the rejection happened at the exact configured cap. Full request-kind
semantics, rejected user execution equivalence, rejected signal source/owned
payload equivalence and precise timer readiness remain open. Ordinary user result
bytes remain opaque. Original full seeded/race, scale, fault, soak and rollout
acceptance remain open. Both larger frozen campaigns are still running their
original sources; this focused run does not replace their qualification. Public
admission/import/online collection remain disabled.
