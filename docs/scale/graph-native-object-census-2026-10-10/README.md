# Native object read uses one fresh configuration/census response

Native Get previously requested stream information twice: configuration-only
admission and filtered chunk census. The filtered response contains both, so
Get now validates its current configuration and exact subject count from one
fresh response before consuming chunk bytes. No admission is cached across
reads. Metadata, receipt identity, bounded size, exact chunk sequence/subject,
size/hash and cancellation checks remain required. The census is taken before
metadata; a concurrent staging completion can therefore cause a conservative
read rejection. Complete attempts have immutable chunk identities. This does
not provide an atomic broker snapshot beyond existing API guarantees.

Development:all seven native object test roots pass under race30.919s.
New R1/R3 two-chunk reads require exactly one Info request each and fresh
admission after an earlier successful read. Unsafe configuration, foreign
subjects, missing chunks and unknown Info fail before any new chunk fetch.
Disabling only census configuration validation makes all eight required
unsafe-config cases return bytes and fail. [Development evidence](development/).
The focused read/cancellation checks pass2.471s.

This removes one native stream-info RPC per Get, not a graph-port operation.
The earlier bounded diagnostic remains62.5 port calls per SetState; no new
latency claim, production-cap qualification or broker-cause attribution is
made. The original live100000 run keeps its source and watchdog unchanged.

## Frozen follow-through

The [driver](run.py) retains source inventories and two race binaries. It runs
the bounded64-entry R1/domain/archive control, all seven native object roots
and eight required unsafe-configuration failures. The [reviewer](review.py)
requires all five actual command exits, exact Git inputs, both binary hashes,
all16 new R1/R3 cells, the exact disabled source, and loaded matching terminal
supervisor exit0. Until that review succeeds, frozen qualification is open.

## Frozen verification accepted

Frozen `af731f4` passes [independent review](review.json):3403 exact Git
inputs, both retained race binaries, all seven native object roots,16 R1/R3
new cells, eight required unsafe-configuration failures, and bounded64-entry
R1/domain/archive continuation control33.164s. All five expected command
exits and loaded matching supervisor exit0 are proven. [Closed evidence](qualified/)
preserves exact source inventories, command state, raw events and mutant.
The original cap run and all broader original requirements remain separate;
this accepts single-RPC Get admission/census, not an overall latency gain.
