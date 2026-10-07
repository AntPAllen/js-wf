# Experimental fenced blob publication protocol

## Implemented decisions

`internal/blobpublication` contains shared Prepare, Commit, Retire and Sweep decisions behind an explicit transport interface. It is not enabled in the runtime. The existing production collector still requires stopped writers.

1. Prepare reads a durable destination head and registers expiring intents before uploading any blob. All blobs in one publication share a unique transaction token. Partial preparation is abandoned on error.
2. Physical names include the content hash, monotonic generation and unique upload attempt. An ambiguous Put is never adopted by reading its bytes. Every new Put invocation uses a new name. One successful immutable attempt becomes the selected object by CAS; shared references reuse that selection.
3. Commit publishes the complete reference set and payload using the original destination head. An ambiguous reply is confirmed only by reading the exact publication token, reference set and payload.
4. Sweep protects committed references regardless of intent expiration. For an expired pending writer, it advances the destination head **while preserving any existing publication** before removing the intent. A CAS conflict triggers another bounded pass; ambiguous failures leave protection for a later sweep.
5. Only an intent-free generation can be closed. Closing races with acquire and upload selection through metadata CAS. Its generation high-water mark remains durable. Later acquisition uses a different generation, so an old Delete cannot remove a new referenced object.
6. Uploading generations protect all their attempts until a selection or closure. Ready generations protect their selected attempt; unselected attempts can be reclaimed. An upload that resumes after closure can produce an orphan, but cannot publish it; a later sweep reclaims the orphan.

No fixed grace period is used as a safety argument. Intent expiration gives the collector permission to revoke a writer by fencing its destination; expiration alone does not forbid Commit.

## Required transport contract

Reads and CAS obey the documented `Port` contract. Destination heads and metadata revisions/generations never reset after retirement, purge, TTL, failover or restart. Successful CAS advances its authority monotonically. Fence updates preserve payload/reference identity. Maps and buffers returned by the adapter are independent copies. Publication/upload IDs are globally unique across processes and restarts. Put success excludes delayed cleanup that could still delete the successful upload; transport retries cannot reuse a name for a subsequent logical upload invocation.

Permanent generation/head high-water metadata is a deliberate storage cost. Pending references do expire and are reclaimed. Reclaiming the authority metadata itself needs a separate durable epoch protocol; deleting it would recreate the ABA bug.

This root model represents one current publication per destination. A retained append-only journal cannot use only its latest subject value as its root census: every retained entry and archived/snapshot/checkpoint reference must remain protected until an authoritative retirement fence invalidates it.

## Local evidence

The retained final command was:

```
GOMAXPROCS=2 GOMEMLIMIT=512MiB go test -race ./internal/blobpublication -count=1 -timeout=3m -json
```

It passed in **11.834 s** at package scope. [Full actual events](race-events.jsonl) and [source binding](local-proof.json) are retained. Hosted CI is configured separately; hosted acceptance is not claimed.

The tests execute these protocol decisions directly:

- 128 virtual-time seeds, 200 generated lifecycle operations each, followed by an exact replay and comparison of every intermediate state digest: 25,600 generated operations plus 25,600 replay operations.
- Paused Commit, paused Put, crashed/partial preparation, retirement, shared and multiple references, generation reuse, preservation of an existing root, and terminal orphan reclamation.
- Lost replies after pin CAS, ready CAS, destination Commit, destination fence, generation closure and Delete.
- Commit winning a fence race; acquire winning a generation-close race; new-generation publication during an old Delete.
- 32 actual concurrent publishers and a collector under the race detector, checking live-reference invariants and final reclamation.
- Negative capability control: deleting the durable destination head admits an expired publication to a reclaimed object, and the invariant checker detects it.

The seeded model copies records, implements native CAS conflicts and immutable puts, and checks every committed reference against actual model bytes, generation selection and protection intent. It does not model NATS implementation internals or prove transport conformance. These focused protocol tests are not added to or substituted for the frozen full123 Tier 1 campaign.

## Remaining implementation and acceptance

- Design and prove durable destination fencing in the actual WF_INV, WF_SIG, WF_JRN, KV state, result and snapshot/checkpoint paths. A separate KV pin alone cannot atomically fence an old stream publication.
- Preserve tombstone/fence authority through current retention and subject purge semantics, including migration and compatibility for existing references. Consumers/replay must distinguish fencing records from workflow events.
- Implement native metadata and ObjectStore adapters; prove successful upload cleanup, immutable attempt identity, lost acknowledgments, late requests, recreation and all retained reference roots. Partial native chunk reclamation needs separate physical-storage evidence.
- Wire every producer and reference retirement path to the protocol; prove full history and shared-object liveness without indefinite pending-reference leaks.
- Add this protocol workload to the common Tier 1 scheduler/trace graph and run its required normal/race gates at the resulting source. Standalone exact replay does not qualify the common graph.
- Run controlled real-cluster active-writer pause/crash/lost-reply/shared-reference cases, then full release gates. Enable online collection only after those adapters and writers satisfy the contract.

Original failed native evidence, ongoing campaigns, million physical drain, dependency adoption and full release/actual24h requirements retain their existing scope.
