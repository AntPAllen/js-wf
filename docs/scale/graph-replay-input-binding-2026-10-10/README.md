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

## Original frozen qualification failed

Source `dab8be3b30b7b10f0d1af06bc2247eacbd62f638` failed under
`js-wf-replay-input-binding-20261010.service` with RemainAfterExit enabled.
[Launch identity](launch.json) records the actual process/invocation. Live
`state.json` remains uncommitted. The [reviewer](review.py) refuses a missing,
unloaded, live or nonzero original supervisor and verifies source/retained
binary/raw event/CLI input identities before acceptance.

The original intended scope includes 65 full SDK race top tests, 12 input-binding controls,
14 prior format controls, 62 metadata and 10 selected-child controls, all 841
normal saved traces, CLI pre-plugin/provenance/legacy plugin tests, six native
exports, eight worker export controls and two required disabled-input-binding
failures. The native both-mode replay/format matrix is preserved, with matching
variant handler selection in this new driver. Two actual old/new input cases
bring the required CLI inventory to 209 invocations. Original prior-driver
failure evidence remains in the separate format qualification.

SDK replay of a worker snapshot supplies both `Format: snapshot.Format` and
`InputHash: snapshot.InputHash`, alongside its identity and object map. The
handler must receive the corresponding snapshot input bytes. This contract
binds those supplied bytes to the journal declaration; it does not authenticate
an attacker rewriting the entire bundle and every declaration together.

## Legacy repair and separate retry

Original supervisor exit 1 and partial commands are preserved in
[failed-dab8be3](failed-dab8be3/failure-receipt.json), including the as-executed
runner, reviewer and launch identity. Empty or opaque legacy Started payloads
do not declare an input hash. Explicit declarations still bind supplied input.
Two provenance fixtures now provide Started for their CLI histories. Directed
SDK, CLI and legacy plugin race tests pass in [development log](development-legacy-fix.log).
The revised runner uses a separate checkout, artifact root and retained service;
its reviewer requires all 15 SDK input controls and the complete prior matrix.
Qualification remains pending.

Retry source `66fe5bc8e49b52b07ff098c9a8be2c8246e14187` is confirmed live under `js-wf-replay-input-binding-legacy-20261010.service`.
[Retry launch identity](launch.json) records its process and invocation.

## Frozen retry accepted

Independent [review](review.json) verifies source `66fe5bc`, 3,344 exact Git
inputs, all six binary identities and actual retained supervisor exit 0. It
accepts 65 full SDK race tests, 15 input controls, 841 saved traces, prior
format/metadata/selected-child controls, CLI provenance/legacy plugin, six
native exports, eight worker export controls, two required mutant failures
and 209 actual offline CLI outcomes. [Retained proof](qualified/state.json)
preserves command results and serialized mutations. Original dab8be3 exit 1
remains separate and unaccepted. Later envelope admission is not covered.
Broader original gates remain open.
