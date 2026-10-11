# Independent canonical signal audit — 2026-10-11

`CheckGraphJournals` now checks current canonical reservation and queue records:
request generation/identity/key, reservation coordinates and token uniqueness,
one exact owned body, byte size/hash, queue equality to the retained reservation,
unique binding, source sequence order/frontier and complete cursor census.
Canonical journal consumption must own its input bytes and match the next queue
entry's index, token, name, sequence, reference and hash. Validation is deferred
until all named forests have been independently traversed. No production graph
reader, signal validator or retained index lookup is called.

Duplicate checks use maps, avoiding quadratic comparisons at larger cohorts.
The audit retains descriptors/events but loads payloads individually.

## Completed evidence

```sh
go test -race ./integrity -run '^TestRawGraphSignalReservationAndBinding$' -count=1 -v -timeout=3m
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=8m
```

Actual exits: both 0. Integrity 1.319 s; worker 88.026 s. Four positive raw
cases cover JSON/protobuf-v1 and both unconsumed/consumed queue entries. All 21
negative controls first pass the independent physical reference audit, then fail
the semantic journal audit. They include generation, reservation, body, queue
and consumption corruption, missing markers and missing journal ownership.
Opaque index bytes deliberately keep persistent index semantics outside this
claim; the positive fixture is a wire-level graph audit, not an SDK replay.

Six actual SDK cases cover R1/R3Domain normal, child promise and buffered child
promise flows. Each finishes with the raw journal receipt; four distinguish the
retired child projection from the fully audited parent journal. The retained
native executable receipt records actual PID211980, its hash and race build flag.
All captured native source hashes were rechecked unchanged. The expanded raw
control fixture was added after native compilation and verified separately.
The earlier integrity command also passed the metadata and journal fixtures in
1.268 s. Logs, source hashes and scoped review are retained here.

CI now selects the signal test and requires every positive and corruption case.
Those new guard assertions were executed against the actual expanded log.
This does not claim execution of the entire combined raw-reference CI job.

## Remaining scope

Persistent signal index packet structure and lookup correctness, original
WF_SIG source-frontier ordering/history, protected reader application replay,
complete wire-schema validation, materialized SDK history reconstruction,
orphan projection census, full scale audits and all-peer physical durability
remain open. Deleted source messages are not reconstructed from queue bindings.
No public admission, import, collection or runtime policy is enabled by this
checker extension. Both frozen qualification campaigns use their original
sources and therefore do not qualify this later audit change.
