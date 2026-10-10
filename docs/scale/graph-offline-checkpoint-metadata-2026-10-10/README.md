# Offline canonical checkpoint metadata validation

The retained real native export diagnostic at `02c3506` accepts a valid control
and six invalid worker metadata variants: missing object, changed bytes, wrong
hash, and rehashed identity, anchor and schema version changes. All still return
completed60 offline, with no effect marker. [Actual old-reader receipt](old-diagnostic.json)
and [diagnostic driver](diagnose.py) preserve this independent runtime defect.

Shared SDK replay now validates each annotated checkpoint completion's metadata
reference/hash, strict unambiguous envelope, version, invocation identity,
anchor and frame hash. It verifies the SDK frame and locals declaration, exact
prior child requests, canonical consumption progress and buffered child signal
metadata against earlier consumption and frame payloads. The CLI calls the same
validator before opening a plugin. Missing objects retain their specific error.
Unannotated legacy completions retain their existing frame validation path.

Directed controls include healthy empty/buffered/legacy cases, missing or
changed bytes, independently rehashed contradictions, duplicate/case-aliased
keys, unknown fields, foreign child declarations and buffered signal changes.
The existing SDK child selection controls and legacy plugin remain covered.
Source-qualified individual SDK/corpus/native CLI command results are reviewed
below. Development tests alone are not full acceptance. [Runner](run.py).

This validates supplied exported bytes, not external storage ownership or
cryptographic authenticity of a rewritten bundle. Full missing-marker/import,
native fault/retention/admission matrices, running-effect cancellation, actual
cap variants, full latest-source seeded/extended qualification, public admission,
production collection and broader original requirements remain open.

## Source-qualified individual command evidence verified — 2026-10-10

Frozen `c6063ab2ee8cbc10d5f29525a91af262d109fcea`: independent
[executed review](review.json) verifies all 3,331 selected Git inputs, unchanged
before/after bytes and five matching retained binaries. The complete SDK race
suite passes all 63 top tests, including 62 metadata and 10 existing selected
child controls. All 841 saved traces pass in normal mode. The 20 CLI provenance
controls and legacy continuation plugin pass. A source overlay disabling only
metadata validation causes exactly two required failures.

All 110 offline CLI command results are verified: 48 healthy native replays,
28 missing-object and 20 missing-stage controls, six new malformed-metadata
rejections, six required old-reader acceptances and two valid diagnostic
controls. New missing metadata reports the specific missing-object error; other
contradictions report corrupt protocol. No effect marker appears. Native inputs
are byte-matched to the independently qualified `ccc8f19` producer; no native
tests or failed latency gates repeated. [Closed raw records](qualified/) and
[retained binary manifest](qualified/binaries.json) preserve these results.

### Supervisor receipt correction

The original supervisor was automatically unloaded after closing. A
`LoadState=not-found` unit displays default zero/success values, which **do not
prove its actual exit status**. The initial reviewer refused this missing
invocation identity. The corrected review verifies recorded child command exits
and source/raw evidence independently and explicitly records original supervisor
exit **unknown**, without claiming the whole runner was qualified.
[Initial refusal](review-initial-receipt.json) and
[correction including earlier affected transient services](supervisor-receipt-correction.json)
are preserved. The earlier selected-child reviewer likewise corrects its
supervisor claim; its actual SDK/corpus/CLI evidence remains verified.

Both still-live broad campaigns now have `RemainAfterExit=yes`, applied by a
service drop-in and daemon reload with their existing PIDs/invocations unchanged;
neither process was restarted. [Live retained-service observations](live-service-exit-retention.txt)
record the change. Future actual terminal exits can be read from retained units.
The 100,000-entry production-cap and frozen156 race campaigns remain unaccepted.

These individual results do not close full missing-marker/import, native
fault/retention/admission matrices, latest-source seeded/race/extended campaigns,
other actual-cap variants, public admission, production collection or broader
original requirements. The goal remains active.
