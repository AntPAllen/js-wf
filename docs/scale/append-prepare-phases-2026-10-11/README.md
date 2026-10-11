# Append preparation and acquisition phases — 2026-10-11

The native active10 race reached Signal binding preparation, then returned a
request deadline while the parent remained healthy. That result is closed in
[its own receipts](../native-expired-pull-fix-2026-10-11/README.md). It identifies
preparation, but does not identify an authority read, intent write, upload,
selection, or node operation. This change supplies those error phases.

AppendPrepareError wraps the primary error at preparation context/configuration,
head observation/check, stream selection, publication token, owned-payload
validation, payload acquisition or node staging. AcquisitionError adds intent
observation/validation/publication, upload-token creation, immutable upload,
uploaded-intent observation/validation, object selection and contention budgets.
Both unwrap only their original primary error. The protocol's calls, validation,
conflict retries, bounds, deadlines, publication/readback and adoption rules are
unchanged. Errors from wrappers that reject application metadata before entering
preparation retain their existing errors.

Twenty-three deterministic cuts drive the actual protocol over its memory port.
They include context/configuration/head/token/owned failures, payload and node
reads, rejected and committed-lost intent writes, rejected and committed-lost
uploads, rejected and committed-lost object selections, and append-spine reads.
Each verifies phase/classification, no exposed prepared plan, no canonical root
write/change, and no retry of the uncertain transport operation. Fresh recovery
uses the original witnessed head and newly granted physical attempts; it never
adopts a physical object left by the cut.

Six retained actual SDK executions pass: protocol, client and exact pinned-corpus
replay in normal and race modes. Each mode has99 named protocol passes (including
all23 new cuts and existing publication/interleaving controls),22 client passes,
and947 corpus passes including all946 unchanged pins. All simulation Go/pin
inputs are compared with d322671; no family, pin, schedule or model is changed.
`review.py` verifies captured input bytes, actual retained binaries, terminal
receipts, named coverage and compiled CI Python blocks. The CI phase step runs
and requires all23 new cuts in its normal/race matrix. The initial unused-import
build failure is retained separately.

This is diagnostic fidelity and protocol safety qualification, not a native
cause assignment, native scale pass, or full current-source seed campaign.
