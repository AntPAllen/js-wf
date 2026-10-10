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
Source-qualified full SDK race, saved corpus and retained native CLI checks
follow via [durable runner](run.py); development tests are not full acceptance.

This validates supplied exported bytes, not external storage ownership or
cryptographic authenticity of a rewritten bundle. Full missing-marker/import,
native fault/retention/admission matrices, running-effect cancellation, actual
cap variants, full latest-source seeded/extended qualification, public admission,
production collection and broader original requirements remain open.
