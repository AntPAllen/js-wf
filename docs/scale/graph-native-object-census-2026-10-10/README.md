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
