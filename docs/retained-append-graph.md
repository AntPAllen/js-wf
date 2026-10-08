# Immutable append graph for canonical storage

`internal/retainedgraph` stages a persistent binary forest. A root has at most63 frontier links, a population and a schema. Each leaf contains bounded metadata and exact external payload receipts. Each branch contains two exact physical child receipts. Hashes cover the canonical node bytes, including absolute index and height. Readers validate every consumed position and digest; unknown/duplicate fields, alternate encodings, missing nodes, wrong physical identities and oversized values fail closed.

Appending stages a leaf and merges equal-height frontier trees, reading and writing at most logarithmically many nodes. Unchanged subtrees are shared. No full history or flat blob inventory is rewritten. Point reads follow one path. A complete graph census streams nodes and payload edges with logarithmic traversal space. The100000-record control checks cumulative storage, per-append work, root size, point-read work, all node/payload edges, boundary records and old snapshots. Failure controls cover committed upload reply loss, bad receipts, cancellation, missing/corrupt/malformed nodes, bounds, callback errors and concurrent staged forks.

The Store contract requires immutable uploaded bytes and exact physical receipts. Upload errors never adopt objects by observing their presence. Store reads must obey the byte limit and caller context. References identify content hash plus physical generation/name; an encoding check is not evidence of existence or ownership. Nodes support64KiB metadata and128 external payload references. Large runtime inputs/results are external objects, not constrained to the metadata limit.

## Bounded receipt membership

`FindTree` resolves an aligned `(first,height)` coordinate by validating only its ancestors. `ContainsNode` compares the exact content hash and physical generation/name; `ContainsBlob` reads one leaf and checks its bounded external edge list. These checks require logarithmic node reads and do not flatten the graph. The target itself may be missing while its canonical receipt still protects it. Missing/corrupt ancestry and storage failures are uncertainty and must stop a collector. Only coordinates outside a validated snapshot establish certain absence; even a storage adapter returning the index sentinel must remain an error.

These are snapshot queries, not durable ownership grants, publication decisions or GC fences. To use them for collection, each expiring intent still needs its destination, original head and exact node/payload coordinate. A collector must read that destination's canonical graph, protect exact reachable receipts across appended root replacements, and fence an expired original head before closing an otherwise unreferenced generation. Publication must validate inherited receipts and acquire all new node/payload intents before upload, preventing arbitrary subtree adoption. Versioned native authority isolation and high-water marks are still required. None of this protocol is implemented by the membership helpers.

[Seeded census, physical identity, uncertainty and budget evidence](scale/retained-graph-membership-2026-10-08/) covers the helper APIs. It does not qualify online collection.

## Bounded append inheritance validation

`ValidateAppend` checks a single-record extension against the exact base frontier. It preserves unchanged frontier receipts and every inherited left child while reading only the new right spine. The returned `AppendDelta` names at most63 new nodes and copies the appended record's bounded payload edges, without flattening the old history. A coordinator can use this delta to check the new upload intents before its root CAS.

The caller still must obtain base from its canonical destination/head, acquire/register durable intents before uploads, verify all delta receipts against those intents, and handle uncertain publication replies. An arbitrary base or structural success confers no ownership. This helper does not independently audit unchanged inherited bytes. [Seeded delta/census and hostile-inheritance controls](scale/retained-graph-extension-2026-10-08/) cover this structural contract.

## Experimental publication coordinator

`internal/graphpublication` now prototypes intent-before-upload, inheritance/grant validation, original-head publication CAS and graph-aware collection over a typed linearizable Port. Collection protects earlier receipts across later appends, fences expired pending publishers, retains closed generation records and uses immutable physical names to isolate delayed deletion. Each publication/content pair has its own permanent authority scope and at most one bounded grant, capped at32KiB; repeated content does not grow a global intent map. Fresh payloads are staged per publication. `PrepareAppendWithOwned` can reuse an exact payload edge from the original canonical graph after verifying its source and ready origin grant. It rechecks at commit without copying bytes or growing origin grants. This is safe for append and whole retirement; partial compaction must transfer ownership before removing origin leaves. Reader/retention pins remain unimplemented. [Reuse controls and limits](scale/graph-owned-payloads-2026-10-08/).

[Seeded model and interleaving evidence](scale/graph-publication-2026-10-08/) covers an isolated deterministic memory Port. [Shared seeded transport and17 exact replay modes](scale/graph-shared-transport-2026-10-08/) now exercise this coordinator through common trace dispatch and pins. Wider fault combinations and complete127-suite qualification remain open, alongside native immutable object adapters, full quorum/permission/fault qualification and schema/namespace rollout, migration and canonical runtime adoption. The existing direct-reference collector cannot enumerate this protocol's objects. Production collection remains quiescent.

## Adoption requirements

The package does not publish authority roots, change workflow storage, supply a native Store, or enable online collection. Its returned roots and competing forks are pending publication plans. A root supplied to Append must come from the caller's canonical authority; unchanged subtrees are not rescanned on every append. This is necessary for bounded append work and is not a complete audit of inherited storage.

The current blob collector protects only direct root references and matches the publication token. Publishing the forest frontier through that collector would leave transitive children unprotected. Integration therefore still requires:

1. A graph-aware ownership and reclamation protocol with a shared deterministic transport model. It must preserve inherited child generations across root replacement, fence every pending append at its original canonical head, fail closed on uncertain reads/replies/missing nodes, and prevent stale or cross-destination subtree adoption. A walk is not a collection fence.
2. Versioned native authority envelopes and fail-closed rollout. Old readers/collectors must reject the new graph representation before any live root uses it; permanent root/blob high-water marks cannot reset. Native immutable node uploads must register expiring intents before bytes and retain ambiguous attempts for safe reclamation.
3. Canonical invocation input, signal, journal, state, terminal, continuation and retained history references. The authoritative root must include the complete live object graph and logical epoch/index metadata. Every writer, reader, reconciler, snapshot/import path and retention operation must use it before production collection changes.
4. Context-bound CAS publication and unknown-outcome reconciliation. Only exact acknowledged canonical publication or its exact quorum-witnessed readback can adopt a staged fork. Native journal fencing must preserve the logical journal index independently of physical witness writes.
5. Complete legacy import and quiesced adapter migration, graph-aware active-writer GC, durable retention/read behavior, native partitions/lost replies/process and power loss/concurrency/scale qualification, and the original complete matrix/24h/million-drain gates.

Existing production collection remains quiescent. Tests of the graph's data structure do not establish any of these ownership or migration requirements, NATS durability, runtime scale or full release acceptance.

## Native metadata authority component

The graph prototype now has an experimental versioned native metadata authority on a dedicated JetStream stream/prefix. Same-subject conditional read witnesses preserve physical-sequence fencing and permanent logical heads/scoped generations; direct and graph adapters reject each other’s envelopes. Strict bounded canonical decode and stream retention/admission checks fail closed. R1/R3 restart, stale GET/absence, lost reply, malformed-wire and encoding controls pass, alongside complete package regression at its recorded source scope. [Evidence and exact source limits](scale/graph-native-authority-2026-10-08/).

That metadata source implements only `Authority`. The subsequent isolated native graph Port adds object uploads, staging reservations, tombstones, bounded physical reads and native collection controls; deployment permission checks and end-to-end fault/scale qualification remain open. Reader retention ownership, partial compaction/import and canonical runtime migration remain required.

## Isolated native graph Port component

The experimental graph `NativePort` now combines native graph authority with a dedicated `graph-recoverable-v1` object bucket. Durable per-attempt staging reservations precede bytes, completion is conditional on the reservation, and collector tombstones fence delayed completion before chunk purge. Read bytes/count/subject/sequence/SHA and context are checked with native message requests; no consumer is created. Graph and direct bucket formats reject each other.

Complete 27-group package regression at0c00893 passes normal20.834s/race77.039s. R1/R3 actual Port controls preserve 17 appended records/one shared payload across sweeps, independently inspect raw child JSON, reclaim all live objects/physical chunks after retirement, reject lost replies and collector-winning completion/publication, and preserve a fresh same-content payload through old-receipt deletion. [Evidence and precise limits](scale/graph-native-objects-2026-10-08/).

Native roles/domain/account permissions, server/process/power loss and real partitions, concurrent and scale campaigns, durable reader retention/partial compaction/import and canonical runtime paths remain unimplemented or unqualified. The runtime's production collector remains quiescent.

## Trusted native graph principal roles

`NativeSubjectAccess` adds only one graph bucket's physical read/upload subjects to the authority allowlist. Native reads create no consumer; both roles therefore deny consumer lifecycle/flow-control APIs. A collector additionally receives the named bucket purge API. R1/R3 controls verify37 exact denials each, unchanged denied-request state, append/reuse/live preservation/authenticated restart and complete retirement/chunk purge. Read-only bucket admission is bounded to three2s attempts after a retained45s admission failure. [Focused final native controls and limits](scale/graph-native-permissions-2026-10-08/).

These are trusted implementation roles. Metadata and message headers can still damage assigned storage; lack of a purge API grant is not an untrusted-principal security boundary. Collector JSON filter/admin ownership/domain/account-import/deployment controls and full native qualification remain required.

## Native shared-handle concurrency

Four native appenders and two collectors race across R1/R3 seeded-release fixtures, with raw canonical value/receipt checks and complete joined retirement/drain. The initial race exposes mutable SDK Stream.Info cache pointer access at nats.go v1.54.0. Each owned native graph stream handle now guards individual Info/Get/Purge/cache calls with context-aware waiting; whole operations and CAS publications remain concurrent. Complete32-group package normal/race regression at5d85031 passes. [Failure report, final runs and precise limits](scale/graph-native-concurrency-2026-10-08/).

This component race is not deterministic replay, complete native linearizability or full concurrency/partition/crash/scale qualification. Review the older direct adapter's shared handles separately, then complete reader retention/compaction/import and canonical runtime migration.
