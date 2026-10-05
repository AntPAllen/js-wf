# Native delivery overhead measurement preparation

Opt-in TestAuditDeliveryNativeCostComparison publishes100,000 x128B messages with
checked ACKs to one R3 file stream. Four fresh memory/AckNone/R3 consumers run
direct Next, the existing byteIteratorBatch adapter, direct Consume callback and
Next recheck. Every path validates stream/sequence/payload and exact fullcount.
EightMiB buffering and2s pull expiry are shared; each path has its own30s diagnostic
limit. All consumers must be removed and the original100k stream retained.

Normal profile GOMAXPROCS2/GOGC100/GOMEMLIMIT2GiB is explicit. Runtime allocations
include linked in-process server activity, and setup is outside the measured path.
This compares transport costs only. It does not qualify400k integrity, cancellation,
leader recovery,24h or a production reader change. Consume candidate integration
would still need the shared invariant/replay/gap/cutoff and cleanup controls.

The producer captures isolated selected Git source, actual SDK bytes/build info,
before/after hashes and closure. Opt-in compilation/skip passes; native execution
is pending.
