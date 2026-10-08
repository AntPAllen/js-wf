# Immutable append graph for canonical storage

`internal/retainedgraph` stages a persistent binary forest. A root has at most63 frontier links, a population and a schema. Each leaf contains bounded metadata and exact external payload receipts. Each branch contains two exact physical child receipts. Hashes cover the canonical node bytes, including absolute index and height. Readers validate every consumed position and digest; unknown/duplicate fields, alternate encodings, missing nodes, wrong physical identities and oversized values fail closed.

Appending stages a leaf and merges equal-height frontier trees, reading and writing at most logarithmically many nodes. Unchanged subtrees are shared. No full history or flat blob inventory is rewritten. Point reads follow one path. A complete graph census streams nodes and payload edges with logarithmic traversal space. The100000-record control checks cumulative storage, per-append work, root size, point-read work, all node/payload edges, boundary records and old snapshots. Failure controls cover committed upload reply loss, bad receipts, cancellation, missing/corrupt/malformed nodes, bounds, callback errors and concurrent staged forks.

The Store contract requires immutable uploaded bytes and exact physical receipts. Upload errors never adopt objects by observing their presence. Store reads must obey the byte limit and caller context. References identify content hash plus physical generation/name; an encoding check is not evidence of existence or ownership. Nodes support64KiB metadata and128 external payload references. Large runtime inputs/results are external objects, not constrained to the metadata limit.

## Bounded receipt membership

`FindTree` resolves an aligned `(first,height)` coordinate by validating only its ancestors. `ContainsNode` compares the exact content hash and physical generation/name; `ContainsBlob` reads one leaf and checks its bounded external edge list. These checks require logarithmic node reads and do not flatten the graph. The target itself may be missing while its canonical receipt still protects it. Missing/corrupt ancestry and storage failures are uncertainty and must stop a collector. Only coordinates outside a validated snapshot establish certain absence; even a storage adapter returning the index sentinel must remain an error.

These are snapshot queries, not durable ownership grants, publication decisions or GC fences. To use them for collection, each expiring intent still needs its destination, original head and exact node/payload coordinate. A collector must read that destination's canonical graph, protect exact reachable receipts across appended root replacements, and fence an expired original head before closing an otherwise unreferenced generation. Publication must validate inherited receipts and acquire all new node/payload intents before upload, preventing arbitrary subtree adoption. Versioned native authority isolation and high-water marks are still required. None of this protocol is implemented by the membership helpers.

[Seeded census, physical identity, uncertainty and budget evidence](scale/retained-graph-membership-2026-10-08/) covers the helper APIs. It does not qualify online collection.

## Adoption requirements

The package does not publish authority roots, change workflow storage, supply a native Store, or enable online collection. Its returned roots and competing forks are pending publication plans. A root supplied to Append must come from the caller's canonical authority; unchanged subtrees are not rescanned on every append. This is necessary for bounded append work and is not a complete audit of inherited storage.

The current blob collector protects only direct root references and matches the publication token. Publishing the forest frontier through that collector would leave transitive children unprotected. Integration therefore still requires:

1. A graph-aware ownership and reclamation protocol with a shared deterministic transport model. It must preserve inherited child generations across root replacement, fence every pending append at its original canonical head, fail closed on uncertain reads/replies/missing nodes, and prevent stale or cross-destination subtree adoption. A walk is not a collection fence.
2. Versioned native authority envelopes and fail-closed rollout. Old readers/collectors must reject the new graph representation before any live root uses it; permanent root/blob high-water marks cannot reset. Native immutable node uploads must register expiring intents before bytes and retain ambiguous attempts for safe reclamation.
3. Canonical invocation input, signal, journal, state, terminal, continuation and retained history references. The authoritative root must include the complete live object graph and logical epoch/index metadata. Every writer, reader, reconciler, snapshot/import path and retention operation must use it before production collection changes.
4. Context-bound CAS publication and unknown-outcome reconciliation. Only exact acknowledged canonical publication or its exact quorum-witnessed readback can adopt a staged fork. Native journal fencing must preserve the logical journal index independently of physical witness writes.
5. Complete legacy import and quiesced adapter migration, graph-aware active-writer GC, durable retention/read behavior, native partitions/lost replies/process and power loss/concurrency/scale qualification, and the original complete matrix/24h/million-drain gates.

Existing production collection remains quiescent. Tests of the graph's data structure do not establish any of these ownership or migration requirements, NATS durability, runtime scale or full release acceptance.
