# Native graph limit profiler error classification — 2026-10-11

The test-only port profiler now records error_kinds alongside total calls/errors
and durations. Exact graphpublication.ErrConflict is classified as conflict;
wrapped conflicts remain wrapped_conflict because they may denote an uncertain
transport outcome. Cancelled, deadline, timeout, revoked, API and other errors
stay distinct. NativeAuthority's direct conflict sentinel arises from observed
head mismatch, definite wrong-last-sequence rejection, changed authority during
mutation retry or exhausted definite witness interference. This does not prove
which of these occurred for a given classified conflict.

Snapshots deep-copy kind counters and deltas subtract counts per kind. The
profiler returns original values/errors and performs no retry or context change.
No runtime source, TTL, request bounds, entry cap or acceptance gate changed.

Focused completed race:1.084s,actual exit0. Eight error classes, concurrent
counter updates, immutable snapshots and value/error identity pass. The native
R1 control commits a root then rejects an old head; its measured receipt is
calls=2,errors=1,conflict=1,wrapped_conflict=0. The new CI job requires both tests
and this receipt without skips; its Python guard was executed against race.log.
The manifest explicitly records source hashes observed AFTER the test, not a
pre-build/binary-bound clean-source qualification.

The historical actual100000 normal recorded17+24 CASRoot errors without class
information. These cannot be retrospectively classified from counts or this
control. Its zero-profile-errors gate remains unaccepted; the verified functional
limit assertions, source/binary and3170 files remain preserved separately. A
future campaign will have classification evidence to evaluate directly. This
focused control is not a replacement for native full-cap race/scale acceptance.
