# Journal and step protocol, version 1

## Encoding and compatibility

The production `WF_JRN` writer defaults to UTF-8 JSON `journal.Entry`
objects. Updated readers also accept versioned protobuf entries. Select new
protobuf writes with `wf-worker -journal-encoding protobuf-v1` or
`worker.WithJournalEncoding(journal.ProtobufV1)`. Existing retained entries are
not rewritten. JSON and protobuf can coexist within an invocation, including
across fencing epochs. A read `journal.Record` adds the physical JetStream
`sequence` outside the stored entry.

Persisted protobuf bytes are the four-byte marker `57 46 4a 00` (`WFJ` plus NUL)
followed by the [version-1 envelope](../protocol/v1/journal.proto). Stored envelope
sequence must be zero: JetStream supplies the physical sequence. Bare interchange
envelopes are not storage messages. After recognizing the marker, readers fail
closed on invalid/unsupported envelopes and never fall back to JSON. Encoding
errors leave decode destinations unchanged. Snapshot manifests and archived
record arrays continue to use their existing JSON formats; compaction can compact
JSON payload whitespace, so arbitrary payload-byte preservation through snapshot
JSON is not claimed. Runtime hash fields still refer to the original input or
Object Store bytes specified below.

Upgrade **every reader** before enabling protobuf writes: workers, clients,
repair scanners, auditors, snapshot/continuation tooling and retention tools.
Older JSON-only readers cannot read protobuf entries. Keep writers on JSON during
the reader upgrade; then enable protobuf writes. Updated writers can return to
JSON while reading mixed history, but rolling binaries back to JSON-only readers
is unsafe while any protobuf entry remains. There is no automatic encoding
negotiation, fleet-version detection or in-place historical rewrite. Focused
mixed-format recovery/compaction and CLI tests pass; full rolling-upgrade and
chaos matrix acceptance for protobuf remains open. The original plan calls for
a Go-only SDK; an additional executable SDK is future scope, not a release gate.

Generate Go bindings with protoc 3.21.12 and protoc-gen-go v1.36.6:

```sh
protoc --go_out=. --go_opt=module=js-wf protocol/v1/journal.proto
```

The schema uses protobuf `uint64` for epoch, index and sequence. Implementations
must retain their full unsigned 64-bit range. The [protobuf wire specification](https://protobuf.dev/programming-guides/encoding/)
defines their varint representation. Generic JSON floating-point numbers are not
an acceptable intermediate representation. The portable vector metadata uses
decimal strings; payloads and wire messages use base64.

Version must be 1, the entry must be present, and kind must be one of the eight
specified values. The current Go projection rejects unknown envelope and entry
fields instead of silently discarding a future requirement. Missing version,
unknown kind, malformed wire bytes, invalid UTF-8 worker IDs and nonempty payloads
that are not valid UTF-8 JSON are rejected. Empty payload means the JSON omitted
field; explicit `null` remains the four bytes `null`. No payload is decoded and
reencoded during interchange. This validation does not prove history ordering,
fencing or kind-specific payload correctness; the journal auditor and SDK do that.

`payload_json` is an opaque byte string containing exact JSON, including whitespace,
large integers and Object Store references. It is not `google.protobuf.Value` or
ProtoJSON. Hashes refer to original runtime-produced JSON bytes, not another
language's reserialization. A port must match the SDK's input serialization before
calling a step. This contract does not introduce a canonical JSON algorithm.
Go's deterministic protobuf marshal stabilizes its own output; cross-language
compatibility is checked by field values and exact payload bytes, not by claiming
that all protobuf encoders produce identical wire bytes.

## Journal identity

| Field | Meaning |
| --- | --- |
| epoch | Lease fencing generation; stale owners cannot append after a successor |
| index | Logical per-invocation journal entry position, including non-step entries |
| sequence | Physical `WF_JRN` stream sequence; zero denotes an unbound interchange entry |
| worker_id | Writer identity, when present |
| kind | Started, StepRequested, StepCompleted, Suspended, SignalConsumed, Attempt, Completed or Failed |

Physical sequence is the expected-last-subject CAS token; it is not the logical
step or entry index. Indices for request and completion are distinct consecutive
journal entries. The SDK also tracks its ordered step position. Snapshot and
continuation offsets must preserve those identities; see
[continuation protocol](checkpoint-continuations.md).

## Run step payloads

`wf.Run` writes a request before executing its effect and writes one completion
before returning the recorded outcome. Other primitives add their own metadata.

| Request field | Contract |
| --- | --- |
| kind | `run` for Run; legacy omitted kind is accepted only for Run |
| name | Stable, exact step name; renaming a recorded step is nondeterministic |
| input_hash | Lowercase hex SHA-256 of the exact `encoding/json.Marshal(input)` bytes |
| duration_nanos, fire_at | Timer duration and retained deadline where applicable |
| clock_domain, timer_step, timer_name | Tagged timer clock identity and timer identity, where applicable |
| child_type, child_id | Stable child workflow identity, where applicable |

| Completion field | Contract |
| --- | --- |
| result | Inline JSON outcome on success |
| result_ref, result_hash | Object Store key and lowercase hex SHA-256 of exact result bytes for a spilled outcome; result must be absent |
| error | Recorded error text; replay returns that failure without executing the effect |
| error_kind | Currently `result_not_serializable` for the typed serialization failure |
| signal_seq, selected | Consumed signal or selection metadata, where applicable |

The inline result limit is 900 KiB. A spilled result is loaded and its SHA-256
verified before exposing it. A missing loader/hash, simultaneous inline and
referenced result, or wrong content hash is corrupt history. Timer deadlines with
clock-domain tags require the configured common clock and repair support; they
must not be interpreted as local wall-clock deadlines.

## Recovery table

These states describe the next step in an ordered journal after the worker has
validated ownership and loaded retained history.

| Request | Completion | Recovery action |
| --- | --- | --- |
| absent | absent | Durably append request, execute effect, durably append completion |
| present | absent | Verify kind/name/input hash, execute effect again, append completion |
| present | present | Verify request, return recorded result or error; no effect execution |
| absent | present | Reject as corrupt history; no effect or new entry |

A crash after the effect and before a confirmed completion can execute the effect
again. External effects need a stable idempotency key where duplicate execution
is unacceptable. The runtime guarantees one recorded outcome, not one physical
effect execution. Changed name, kind or input hash is a nondeterminism error.

Every new journal entry uses expected-last-subject CAS. An ambiguous publish
outcome must be resolved by rereading retained history before retrying an effect
or appending from a stale tail. A lease loss stops appends; a successor obtains a
higher fencing epoch. The interchange codec has no ownership or transport behavior
and cannot replace those worker responsibilities.

## Portable verification

`protocol/record_test.go` checks all eight kinds, unsigned 64-bit boundaries,
exact payload preservation, detached ownership, malformed and future envelopes,
and the four recovery states using the actual Go SDK producer and replay engine.
`scripts/protocol-vectors` regenerates the checked-in fixtures from the production
interchange and storage codecs; CI compares the generated bytes.
`scripts/check-protocol-interop.py` uses independently generated Python bindings
to decode both the bare envelopes and marked storage messages, then constructs
distinct Python interchange and stored records for Go to decode. Storage envelopes
must have sequence zero; the standalone interchange retains physical sequence.
`journal-protocol-interop.yml` runs both directions under the Go race detector and
retains vectors, tool versions and hashes. This is a codec interoperability test,
not proof of a second SDK or a full rolling-upgrade/chaos matrix pass.
The same CI also exercises a real R3 worker resuming a JSON prefix, protobuf
persistence, all-peer audit, snapshot compaction and blob retention, plus the
CLI encoding flag and retention/reuse workflow.
