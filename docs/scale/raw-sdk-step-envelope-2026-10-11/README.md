# Complete SDK step envelope admission in raw graph audit — 2026-10-11

Every raw graph StepRequested/StepCompleted entry now passes a complete envelope
check before journal accumulation or semantic observers. Own request/completion
wire declarations cover all currently emitted runtime fields, including nested
Select cases, timer deadlines/domains, external results and checkpoint metadata.
The shared unambiguous JSON primitive rejects unknown/case-aliased/duplicate typed
fields and malformed shapes; production stepwire decoders and SDK replay are not
called. Metadata fields preserve presence and reject explicit null/non-string
values. Runtime envelope JSON must be an object, not null or an array.

User result bytes remain opaque json.RawMessage. Arbitrary user field names,
case aliases and duplicate keys inside that result are admitted unchanged;
this checker does not reinterpret the user's serialization as runtime metadata.

## Physical fixtures and native compatibility

Four positives per JSON/protobuf encoding cover ordinary results, arbitrary user
field names, opaque duplicate keys and null results. Twenty-four corruptions per
encoding cover root shape, unknown/aliased/duplicate/escaped-duplicate fields,
wrong time/step types, Select case shape/keys, completion types and metadata
presence/type. Every fixture first passes complete physical references. Raw
payload hashes, graph nodes, owned grants and full inventories remain consistent;
all48 corruption cases must fail the journal auditor. Race1.541s also passes the
existing historical checkpoint, metadata and SDK state fixtures.

Actual native race cases exercise R1/R3Domain scheduled positive timer workflows
and buffered child promises, with complete raw checkpoint-history audits. Final
process exits/timing/logs, actual executable and captured source hashes are in
review.json. CI requires all56 new physical fixture cases, and its new subset
guard is executed against the actual focused log. Full hosted CI is not inferred.

## Remaining scope

This checks runtime field admission, not every kind-specific semantic relationship
or handler input/result equivalence. General effect/sleep results, invocation and
terminal/source envelopes, ambiguous promise completeness, clock/case readiness,
protected reader application replay, all-peer/storage proofs and original full
simulation/scale/fault/soak/rollout gates remain open. Unannotated checkpoint state
keeps structural scope. Public admission/import/collection remain disabled. Both
larger frozen campaigns predate this change and cannot qualify it.
