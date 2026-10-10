# Bind replay input to canonical Started bytes

The real retained child export diagnostic at `04f542b` accepts an ignored extra
input field after recomputing the bundle's own input hash. The canonical Started
record still declares the original hash, yet replay returns60. The actual old
CLI/plugin/input hashes, positive control and mutation are [preserved](diagnostic/report.json).
This failure is independent of NATS and handler non-determinism: the handler
ignores the changed field and reproduces the same SDK requests.

Shared SDK and pre-plugin replay validation now bind the supplied InputHash to
Started.input_sha256. Graph-v1 requires both and admits an unambiguous strict
Start envelope. Unversioned supplied hashes verify existing Start declarations;
legacy histories without such a declaration retain compatibility. Worker
snapshots return their verified InputHash, which SDK callers pass alongside
snapshot.Format and Objects. CLI validates the actual input byte hash and passes
that same hash through ordinary, limit and cancellation replay paths.

Twelve SDK controls and two CLI pre-plugin controls cover rehashed inputs,
missing declarations/hashes, malformed hashes, duplicate/case-aliased keys and
legacy compatibility. The as-executed directed development race result is
preserved. Frozen qualification follows with full SDK race, all saved traces,
worker and native CLI exports, both-mode retained native bundles and required
old/new input controls. It does not complete latest-source seeded/extended,
full import/fault/retention/admission, remaining cap variants, public admission,
production collection or broader original requirements. Rewriting every input
and journal declaration is outside authenticity supplied by this local bundle.
