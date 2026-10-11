# Independent persistent signal index audit — 2026-10-11

The raw journal checker now validates the index packet in every current
`signal-input` and `signal-queue` record. Its own decoder checks the wire magic,
length/count limits, reserved bytes, leaf identity/value, branch prefix/bit
ordering, side constraints, backward record/slot coordinates and reachability
of every new node from the packet's last-slot root. The leaf key is bound to
SHA256 of the reservation request and its value to the owning forest index.

For each update, the auditor removes only that new key from the logical tree.
The remaining tree fingerprint must match the preceding complete prefix tree.
This rejects structurally valid packets which forget earlier keys or replace
the earlier tree. Logical SHA256 fingerprints include node type/bit, key, value
and child fingerprints, excluding physical coordinates so copied paths compare
equal. This uses the same SHA256 collision-resistance assumption as the retained
graph itself. It does not use the production retainedindex decoder, lookup,
update, graph reader or admission validator.

Checks process at most 257 newly encoded nodes and a 256-bit tree path per
record. Historical node metadata is retained to resolve older coordinates;
payloads remain individually bounded reads. Full large-cohort resource
qualification remains open.

## Actual completed evidence

```sh
go test -race ./integrity -run '^(TestRawGraphSignalIndexIndependentPrefixes|TestRawGraphSignalIndexProductionPackets|TestRawGraphSignalReservationAndBinding)$' -count=1 -v -timeout=3m
go test -race ./integrity -run '^(TestRawGraphSignalIndexIndependentPrefixes|TestRawGraphSignalIndexProductionPackets)$' -count=1 -v -timeout=3m
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=8m
```

Expanded integrity exit0 in1.571s; final index exit0 in1.600s; native SDK exit0
in95.230s. The independent wire fixture has a valid three-record persistent
tree and21 corruption controls, including lost entire/partial prefixes,
unreachable new branches, malformed coordinates, padding, topology and leaf
identity/value. A cancellation control propagates context.Canceled. The
unreachable-node fixture was strengthened after the first index command;
the final log verifies a genuinely unused branch, rather than duplicate leaves.
Both earlier passing logs and the focused reachability rerun are preserved.

Production generates1024 distinct hashed-key updates for differential checking.
A separate257-key adversarial prefix sequence exercises a256-bit path and the
maximum257-node packet. Production is only the test input generator; all audit
decoding and validation are independent. Four new graph corruptions first pass
the physical reference audit, then fail index semantics. Existing21 graph signal
corruptions and four JSON/protobuf unconsumed/consumed positives also pass.
The earlier positive fixture's opaque index bytes were replaced with an
independently encoded single-leaf index.

Six actual SDK normal/child/buffered-child R1/R3Domain cases finish with the
extended raw journal receipt. The executable receipt binds actualPID212873,
SHA256 and race build flag. The captured336 source hashes include tracked
production Go, worker tests, module files and the new index implementation;
all were rechecked unchanged. Later integrity test fixture changes were
verified in their separate completed commands and have explicit hashes.
This is focused development-worktree evidence, not a frozen latest-source
whole-suite qualification. review.json preserves scope and log hashes.

CI selects both index tests and requires all positives, negatives and the
maximum-depth case. The complete native SDK guard and new signal/index subset
of the raw graph guard were executed against the actual logs; the exact code
is retained here. No execution of unrelated raw graph cases is inferred.

## Still open

Original WF_SIG source-frontier ordering/history and deleted-message provenance,
protected reader application semantics, complete cursor/wire-schema validation,
SDK materialized history reconstruction, orphan projections, full scale audits,
all-peer physical durability and original implementation-plan acceptance gates
remain separate. No public admission, import, collection or runtime policy is
enabled by this checker. The frozen159 race and classified100000-entry normal
retain their earlier sources and do not qualify this change.
