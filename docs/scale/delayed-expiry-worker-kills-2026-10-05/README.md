# Seeded delayed-expiry / successive-owner-loss diagnostic

Clean executed source `e63bb02`: the added workload completes100,000 normal
seed bodies and1,000 race bodies, allnine timing combinations. All392 pinned
regressions replay exactly under race, including existing zero-delay models.
The new seed42 pin is identical across two processes and replays through the
normal trace dispatcher. Actual normal/race model binary SHA/build fields and
1,047 selected Git source/pin inputs independently verified before/after.

The test uses production lease Acquire/Renew/Release and journal Append/Read
against seeded KV and dispatch ports with production TTL12s/AckWait13s. It checks
stale owner renewal/append rejection, unchanged terminal journal and physical
modeled queue drain. It does not execute the full worker handler/heartbeat
or OS processes and does not model NATS Raft/disk/expiry internals.

## What the model establishes

The native failed rollout recorded a held second delivery about13.3s after the
first owner's acquisition. Last renewal/server expiry timestamps were not
retained, so late expiry visibility is **an explicit hypothesis**. The model
varies first-owner visibility delay1.5/3/5s and survivor service cost2.6/2.8/3s.
It compares immediate expiry with a delayed first expiry. Only after that key
actually expires may the delay be cleared for subsequent owners; changing the
assumption while a key remains live is rejected.

With immediate expiry, the successor completes in15.6–16s before the next cut.
With the delayed first expiry, delivery2 finds a held lease and NAKs for5s;
delivery3 acquires at18s, is killed at20.5s before the3s heartbeat, then delivery4
completes in34.1–34.5s. FetchOne's existing1s virtual polling contributes0.5s
following the fractional cut. All timing/cut/poll assumptions are explicit;
these durations are **not an exact reproduction** of native33.911s. Pin42
records16s vs34.5s and one versus two owner deaths.

A persistent delay for every owner adds another held retry, unlike the retained
native sequence. The initial draft exposed that mismatch; the final diagnostic
models delayed visibility of the first generation only. This demonstrates a
missing timing sensitivity in the earlier exact-TTL model, while leaving the
historic cause unconfirmed. It does not turn a missed30s latency gate into a pass.
No production TTL/AckWait/held retry/strict latency target changed.

Next retain the held entry's revision/epoch/creation time and lease operation
observations without extra broker reads. Resolve late expiry versus renewal /
replica-read / dispatch effects before another sustained native run.

Full model proof: 1,060 members / 29,339,824 bytes / 2 parts, all read/hash-verified. SHA256 `e31dd582082bb3657d8eabbe2a7073e9decf66764a8b2720ee6dfe7ea54559ab`.

See [independent review](independent-review.json), [archive verification](archive-verification.json), and [executed producer](executed-producer.py). This qualifies the added modeled workload; full current-source Tier1/matrices/actual24h remain open.
