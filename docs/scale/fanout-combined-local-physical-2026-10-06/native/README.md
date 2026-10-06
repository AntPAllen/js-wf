# Full combined fanout local physical campaign failed

Originalaa2015a race SDK1899324 fullsix selection: FAIL370.79s. Five cases pass
and their three distinct local physical queues/64durables/drain-after-joins are
independently rechecked. Results/first fails40.84s at Worker.New's signal-stream
metadata lookup after the recorded journal restart (node0). No final drain witness
exists for that case. It is not a passing fullsix campaign.

The original case context is5minutes; worker constructor intentionally bounds its
whole startup to5seconds so callers can retry. The fanout harness invokes it only
once after journal-only replica readiness. The retained error proves constructor
startup deadline exhaustion, not the exact request latency/server-side cause.
That cause remains unconfirmed. A caller-side transient startup retry under the
same original case deadline is the next correction; no deadline/cardinality change.

SDK closed,694Go/module/3,286external source inventories and actual executable
rechecked. Full16,793member65,523,988byte/threepart archive readback passes; SHA256
11362c0f0a626cd09d6164e9cc30bc519b6ffbb0b4a10c63ce236c45a5b9d22d.
Original failed stores/source/exes remain closed and preserved. Previous historical
API-only drain scope remains separate. Current original24h/million handles continue.
