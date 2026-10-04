# Upgrade seed 74 failure before fifth replacement

Run37164231641 /job111324440317 at `79915ca41a5c5a23b9997eea5f3f66d82530ee30` fails seed74 in
`fault-5-upgrade-before.json`, before the fifth server replacement. Seeds66–73
producer passes are not independently qualified;75–78 were not executed.

All five native semantic rejection checks and fallback admission complete.
The proof then returns `upgrade WF_RUN stale replica` in replica-readiness,
from11:58:55.469883574 to11:58:56.033578025 UTC. The source first completes its
readiness polling and then performs a separate one-shot WF_RUN metadata read.
The rejected StreamInfo was not retained; the exact replica state and server
cause remain unconfirmed. This does not establish sustained unavailability.

All nine pre/post source captures verify unchanged against exact Git759 inputs.
Raw artifact11302499431 /9,845,569 ZIP bytes and originals artifact11302644144
/367,834,528 ZIP bytes are downloaded completely. All35,151 original member
hashes /1,293,139,435 bytes verify against the original manifest. Canonical
original archive367,524,095 bytes has SHA-256
`57563c118ac074c40fbd76a1c013c2deb11ca47cbf1665bad6370498a5d7449d`.
Originals are not independently reopened. Actual NATS binaries are retained;
the original SDK executable was not uploaded.

Compact proof: 1638 members /378,670,183 compressed bytes /
15 parts. Every member, original input and part/concatenated archive
readback verifies. ZIP wrappers remain local and are excluded from this compact
proof; complete expanded raw files and nested canonical original payloads are
included. No failed shard, parent, full matrix or actual24h qualification.

The fixture now polls dynamic replica readiness within the same original whole
proof60-second context and records each StreamInfo, including rejected states.
Configuration mismatches remain immediate. Race controls cover catch-up,
persistent-stale deadline, cancellation and semantic configuration failure.
This fixture change does not confirm the historical server cause; corrected
native qualification remains pending.
