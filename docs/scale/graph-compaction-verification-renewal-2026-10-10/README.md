# Intent renewal during private final verification

`CompactionCommit.BeginIntentRenewal` pairs a verifier with its exact completed
stage, freezes both and renews all publication scopes through the verifier's
authority port. Full success adopts the extended expiry on both private plans.
Record/node progress is retained in memory; no verification certificate is
serialized. Renewal changes expiry only. Existing immutable-object/grant checks
and the original-head final CAS remain necessary. Conforming collection must
fence that head before revoking pending grants. Concurrent staging/renewal under
the same token remains prohibited.

In-batch errors poison both handles. Upfront cancellation preserves the pending
renewal. A wrong stage, expired operation, published verifier or overlapping
renewal cannot be paired. The journal now permits explicit renewal in its verify
phase and returns there after success. Saved pending input retains the completed
stage and requested expiry; fresh resumption reconstructs staging and restarts
all verification. In-memory progress is never recovered from descriptor bytes.

## Development evidence

Twelve direct controls cover renewal during record/node verification, repetition,
verifier authority selection, wrong stage, cancellation/expiry, append/collector
races and dropped/lost replies. Record/node counters stay fixed during renewal;
full completion compares12 records and validates19 nodes. A stage backed by a
different port cannot redirect renewal away from the verifier's authority.
Freeze/expiry/authority bypasses fail10/5/1 controls respectively; exact unsafe
sources and outputs are retained.

The journal's40 JSON/protobuf binding/recovery controls and11 malformed-envelope
controls pass. Live verify renewal retains the previously checked record batch
and requires9 remaining verification batches. Resumed or unknown-update recovery
requires all10. Native R1/R3 domain cases perform both staging and verification
renewal (25 scope batches total), retain8 verification batches, sweep beyond both
old intent expiries, preserve the old reader and survive the existing two peer
restart cuts. Each verifies two compactions, logical bytes/audit, original receipt
collection and zero retired physical chunks. This is controlled-clock component
evidence, not runtime descriptor-store durability, OS process/VM power/storage
loss or real clock-jump qualification. Native2-minute contexts are unchanged.

The broad graph compaction/native selection and all853 unchanged common saved
traces pass. Run
`python3 docs/scale/graph-compaction-verification-renewal-2026-10-10/review.py`
to inspect fixed sources and exact retained terminals. Prior reviews are frozen
at their own commits.

## Remaining work

Worker selection/persistence, lease-aware renewal scheduling, durable maintenance
input and recovery, bounded-memory namespace enumeration and long maintenance
deadlines remain open. Private progress survives successful in-process renewal
only. The worker still uses its15-second whole handoff deadline and fixed intent
expiry. Actual100000 and all broader original qualification gates remain open.
Public graph continuation admission stays closed; production collection stays off.
