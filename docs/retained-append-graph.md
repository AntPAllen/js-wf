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

`internal/graphpublication` now prototypes intent-before-upload, inheritance/grant validation, original-head publication CAS and graph-aware collection over a typed linearizable Port. Collection protects earlier receipts across later appends, fences expired pending publishers, retains closed generation records and uses immutable physical names to isolate delayed deletion. Each publication/content pair has its own permanent authority scope and at most one bounded grant, capped at32KiB; repeated content does not grow a global intent map. Fresh payloads are staged per publication. `PrepareAppendWithOwned` can reuse an exact payload edge from the original canonical graph after verifying its source and ready origin grant. It rechecks at commit without copying bytes or growing origin grants. This is safe for append and whole retirement; partial compaction must transfer ownership before removing origin leaves. The experimental reader protocol below now supplies bounded durable snapshot pins; canonical reader migration remains open. [Reuse controls and limits](scale/graph-owned-payloads-2026-10-08/).

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

## Shared adapter stream-cache guard

The older direct adapter reproduces the same shared SDK stream cache race. Both native adapters now use `internal/natsstream.Guard` for their owned handles; the existing waiting-context test lives in that shared package and both CI workflows include it. Complete helper/direct/graph packages pass normal/race atd6ef52f, with graph31 groups and no coverage removal. [Exact source/input timing and regression limits](scale/native-stream-cache-2026-10-08/). Reader retention/compaction/import and canonical runtime migration remain the next adoption work.

## Experimental durable reader snapshots

`AcquireReader` captures the current canonical live graph at the original head, registers up to eight durable snapshot pins, and permanently upgrades that destination to `js-wf-graph-publication-retention-v2`. Each graph retains its bounded frontier; the complete root is capped at256KiB and native authority envelope at260KiB. The32KiB scoped grant bound is unchanged. Append preserves the pins. `RetireLive` removes only the live graph and preserves snapshots; the original v1 `Retire` operation is rejected by every adapter after schema upgrade. Existing v1 publication traces remain unchanged.

`RenewReader` extends an existing exact snapshot and cannot reattach a removed ID. `ReleaseReader` removes that pin by original-head CAS. Collection checks both live and retained graphs. Before ignoring expired pins, it removes them through a root CAS that advances the permanent head. If renewal wins, collection rereads the renewed pin; if expiry wins, the paused renewal loses its original head. Unknown expiry acknowledgements stop collection before object deletion. Reader mutations resolve unknown replies only through exact original-head image readback. An ancestry/read error stops collection.

`ReadRetained` verifies the canonical lease before reading; the caller must finish all snapshot/payload reads before expiry or renew first, using a collection-consistent clock. An expired read can fail while collection runs; observing bytes cannot extend a lease. `ExpireReaders` fences pins at a known destination, including empty snapshots with no object grants. Canonical runtime migration must retain and enumerate that destination catalog. Object enumeration alone cannot supply it. Returned snapshots and pin slices are independent copies; arbitrary old forests cannot be passed to acquisition.

Twelve shared-transport modes cover retirement reads, release, expiry, both renewal/collection winners, both acquisition/retirement winners, lost acquire/renew/release/expiry acknowledgements and uncertain ancestry. They generate and exactly replay1,000 schedules, with12 new pins under a separate workload. Existing17 publication pins still replay unchanged. Current source inventory is128 seeded workloads/462 pins; complete qualification of that inventory remains pending. Isolated native R1/R3 controls retain original bytes across retirement, adapter reopen and renewal, then reclaim objects/chunks after expiry while retaining schema/head fences.

This is a reader ownership component. It does not qualify old deployed adapters' rejection, provisioning/deployment rollout, server/process/power-loss retention, partial compaction/import, canonical input/signal/journal/state/terminal/continuation/history migration, the complete native matrices/24-hour/million-drain gates, or production online GC. Production collection remains quiescent.

Complete package verification at7a9ea04 passes graph38 groups normal27.856s/race113.461s and the shared guard. Reader replay passes100,000 normal schedules/1,000 race schedules,12 modes, and all29 publication/reader pins in101.529s/13.938s. [Complete logs, input check and exact component limits](scale/graph-readers-2026-10-08/). Reader handle persistence/resumption and crash qualification remain required in addition to canonical migration.

## Reader checkpoint recovery

`Reader.Checkpoint` now serializes a canonical bounded descriptor (up to2KiB) containing only the destination, globally unique reader ID and SHA-256 of the exact canonical snapshot frontier. It carries no graph image, head or lease expiry. `ResumeReader` validates its strict encoding, obtains the destination through a quorum-witnessed canonical read, and restores only an unexpired exact ID/snapshot match. Missing, released, expired, foreign or changed snapshots fail closed even while physical bytes remain. A replacement pin for the same graph cannot authorize an old checkpoint. Resumption does not advance the logical head or extend expiry; native read witnesses still write their physical sequence.

Model controls cover strict unknown/duplicate/alias/trailing/oversize rejection, the complete admitted destination byte length with JSON escaping, cancellation/uncertain reads, renewed expiry, copy independence and identity revocation. Nine new shared-transport modes generate/exactly replay1,000 schedules and preserve9 new pins, with all29 previous graph publication/reader pins unchanged. Current inventory is129 seeded workloads/471 pins; complete current-source suite qualification remains pending.

Native R1/R3 controls save checkpoint bytes, retire the live graph, stop every peer before any is restarted, then reopen the original file stores and a fresh adapter. With the original in-memory Reader discarded, canonical resumption recovers record and payload bytes, observes renewal, rejects release and drains objects/chunks. This is graceful full-store restart, not an actual reader process kill, SIGKILL/power loss or provisioning/deployment rollout qualification. Complete package normal/race and extended seeded verification follow at the committed source. Canonical destination catalog/reader/writer migration, compaction/import and the broad original release gates remain required.

At frozena0630ca, complete graph42-group normal/race regression passes41.149s/118.592s. Recovery exactly generates/replays100,000 normal schedules and1,000 race schedules across9 modes; all38 publication/reader/recovery pins pass unchanged in74.626s/11.158s. [Full logs, selected-input checks and component limits](scale/graph-reader-recovery-2026-10-08/). Canonical destination catalog/runtime migration and the remaining crash/rollout/compaction/release requirements are still required.

## Authority destination catalog and complete reader sweep

The experimental native authority now discovers root destinations through its permanent isolated metadata stream. `RootKeys` verifies a complete subject census, count/namespace/hashed identity and strict bounded envelope before returning any names. It includes empty roots and witnessed absences. Its tentative GETs discover identity only; `SweepWithReaders` subsequently uses witnessed `ReadRoot` and expiry CAS for each destination before the object-grant sweep. Incomplete, malformed, foreign or uncertain catalog/root/expiry results stop collection. Root identities and schema/head fences survive empty-snapshot expiry and complete object reclamation. Concurrent roots entering after a census are retained for the next sweep.

This complete reader lifecycle also handles roots with no object grants or only closed grants. Renewal conflicts reread the canonical pin; unknown expiry replies stop before grant closure/deletion. A port without catalog support fails closed. The original object-grant `Sweep` remains for historical replay; canonical graph adoption must use `SweepWithReaders`. Both trusted named native roles can enumerate destinations using their existing scoped API permissions.

Native R1/R3 controls expire12 roots with8 empty pins each, preserve a retired live snapshot/payload, then reclaim its chunks while retaining destination identities. Eleven native malformed/partial/unknown census controls reject before expiry; no logical head changes. Nine shared transport modes generate/exactly replay1,000 schedules, with9 new pins and all38 previous graph pins unchanged. Current inventory is130 seeded workloads/480 pins; complete current-source qualification remains pending. Complete package normal/race and extended catalog verification follow at the committed source.

This adds an isolated authority catalog, not canonical invocation/history discovery or runtime reader/writer migration. Census paging/concurrent-admission/large-population qualification, old-deployment rollout, compaction/import, abrupt process/power loss and all original broad native/matrix/24h/million-drain/online-GC requirements remain open. The original frozen full126 normal100k campaign is now independently accepted; it cannot qualify newer graph/native/reader/catalog changes.

At frozenf077727, complete graph47-group normal/race verification passes33.594s/114.950s. Catalog transport passes100,000 normal/1,000 race schedules across9 modes with exact replay and all47 graph pins unchanged in88.944s/14.666s. All1,363 selected inputs matched Git before execution and remained unchanged afterward. [Complete logs, terminal review and limits](scale/graph-catalog-2026-10-08/). Canonical runtime adoption and catalog native paging/large-population/concurrent-admission qualification remain required.

## Application lifecycle descriptor

Experimental root schema `js-wf-graph-publication-application-v3` adds an opaque descriptor of at most 4,096 bytes. `PrepareAppendWithApplication` publishes its copied bytes in the same original-head CAS as a new graph leaf. Ordinary appends preserve the descriptor; `UpdateApplication` updates it without changing graph ownership. Reader acquisition, renewal, release, expiry and `RetireLive` preserve it, including after every graph object is reclaimed. The v3 schema remains permanent after clearing the descriptor; adapters reject v3-to-v2/v1 downgrades.

This supports a future versioned journal cursor/generation/epoch/terminal descriptor whose logical index must survive graph retirement and remain independent of root heads advanced by reader operations. The graph protocol does not interpret those fields. Bytes or receipts mentioned only in the descriptor establish no object ownership; exact graph edges and canonical reader pins remain necessary. Callers must validate their application schema and lifecycle transitions. Lost application-update replies resolve only from an exact witnessed image at the original head plus one; no uncertain mutation is retried.

The journal adapter and canonical runtime paths are not wired yet. Complete migration/import/retention, deployed old-adapter rejection, partial compaction, crash/power-loss and full native release gates remain open. This addition keeps production collection quiescent.

## Generation-aware graph journal

`journal.GraphStore` is an explicit-generation adapter over the isolated graph protocol. `Begin` accepts a caller-verified invocation sequence; it rejects implicit legacy import and generation replacement before terminal retirement. Each canonical application cursor stores version, invocation sequence, base sequence, count, epoch, last kind and retirement state, using bounded strict canonical JSON. Logical sequences are base plus index plus one, independent of physical authority heads. New generations reset logical indices but preserve the sequence high-water mark, and old generations cannot append or open current history.

`Append` validates the cursor, expected logical tail, contiguous index, known kind, epoch monotonicity, first Started and terminal closure. Encoded JSON/protobuf entry bytes are exact graph-owned payloads capped at 1MiB; leaf metadata remains bounded. Additional external input/result objects require explicit uploaded payloads or original live-graph owned edges. Opaque JSON object names are not ownership. `Retire` atomically publishes the retired cursor and clears the live forest using `RetireLiveWithApplication`; only Completed/Failed generations may retire. Reader pins and cursor survive complete object/chunk reclamation.

`Open` captures a durable immutable `GraphView`, whose reads validate entry envelopes, generation, logical sequence, entry digest/index/kind/epoch and lease boundary. `Payload` accepts only an exact edge from the specified retained record. Renewal/release use original-head CAS; expired or closed views cannot read or renew. `Read` checks complete contiguous history and holds/renews its view until returning decoded entries, then releases it. Consumers that use external payloads must keep a GraphView open. Failed/cancelled cleanup can leave only a bounded expiring pin. Views are not safe for concurrent mutation.

The adapter does not establish that a supplied invocation sequence is current in WF_INV; trusted callers must establish canonical invocation identity and purge ordering. Worker, SDK blob/state/outcome, signal/reconciler, checkpoint/snapshot, import and retention integration are still required. The existing worker uses the legacy Store. Native tests reopen adapters without restarting server stores; this is not process/power-loss qualification. Production GC remains quiescent.

## Opt-in graph worker and terminal client

`worker.WithGraphJournal` binds each delivery to its observed invocation sequence and holds a GraphView for execution. The Started entry owns the input digest and payload. Result-store callbacks stage new bytes in delivery-local memory; the referencing StepCompleted/terminal append atomically publishes their graph edges. Replayed input, results and previously consumed external signals resolve only exact owned edges; a missing graph edge does not fall back to the original staging object. New external signals are read from their retained source and transferred into the journal when consumed. Failed journal-limit signal declarations retain their referenced bytes too.

Successful appends replace the delivery's view without scanning the whole history again. Payload reads refresh expired views only from the current matching generation. Graph operations have the worker's bounded contexts, including a15s append/payload budget and3s bounded pin cleanup. `GraphConfig.PayloadReadLimit` selects an explicit byte budget, default64MiB; configure a larger bound consistently for larger runtime payloads. `GraphRecord.EntryBlob` exposes the exact encoded-entry edge when identical entry/external bytes deduplicate.

`client.NewWithGraphJournal` resolves large terminal results through the last Completed record and its exact payload edge, checking the observed state's generation/reference/hash against that record. Terminal duplicate probes use graph history as well. This mode skips legacy journal auto-snapshots and rejects continuation registrations after all options are applied. The default worker/client paths remain legacy.

This is a worker/journal payload migration component, not complete canonical migration. Initial start/input/signal publication and terminal state still use legacy metadata; their current reads are not newly qualified quorum witnesses. Snapshot/continuation/import, client signal/start/reconciler/retention/history paths, schema deployment and full native/scale/crash/power-loss gates remain open. Production online GC stays disabled. Callers must coordinate retirement with invocation/state purge; the native test removes them explicitly before retiring the closed fixture.

### Reader cleanup contention

GraphView.Close retries at most sixteen definite root CAS conflicts, reading a fresh authority head before each attempt. Each release still names the exact original reader token and snapshot. Other errors, including uncertain acknowledgement/readback, stop immediately; ambiguous release keeps the protocol's original-head reconciliation. This prevents a concurrent worker/client pin release from discarding an already validated terminal result solely due to definite metadata contention. Open and Renew also retry at most sixteen definite CAS conflicts. Open re-observes the cursor and invocation on every attempt, so retirement or generation replacement rejects rather than attaching an old cursor to a new snapshot. Renew checks local expiry before each attempt and again after the authority read; expired handles cannot be resurrected. Unknown acquisition/renewal errors are never retried. Further native concurrency/capacity qualification remains required.

### Public native graph SDK configuration

Applications can now configure the opt-in graph worker and terminal client without importing Go `internal` packages:

```go
cfg := journal.NativeGraphConfig{
    AuthorityStream: "WF_GRAPH_AUTH",
    AuthorityPrefix: "wf.graph.runtime",
    ObjectBucket: "WF_GRAPH_OBJECTS",
    ExpectedReplicas: 3,
    PinTTL: time.Minute,
    IntentTTL: time.Minute,
    PayloadReadLimit: journal.DefaultGraphPayloadLimit,
}
graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
if err != nil { return err }
w, err := worker.New(ctx, js, workerID, handlers, worker.WithGraphJournal(graph))
if err != nil { return err }
c, err := client.NewWithGraphJournal(js, graph)
```

`OpenNativeGraphStore` requires existing, isolated stores and validates the graph authority and recoverable object format. It never provisions, updates, imports or collects. `ExpectedReplicas` greater than zero requires both streams to match; zero accepts the underlying adapters' structurally safe positive replica counts. Stream/bucket names use letters, digits, underscores or hyphens; bucket names are at most32 bytes. Authority prefixes are dot-separated tokens from the same alphabet. Zero TTLs/payload budget select the existing graph defaults; zero encoding selects JSON.

For **new** isolated deployments, an administrator can obtain the exact authority/object configurations with `journal.NativeGraphStreamConfigs(cfg, replicas)` and explicitly provision them with `js.CreateStream`. Replica counts must be1–5 and match a nonzero `ExpectedReplicas`. Existing streams must be preserved; this helper does not provide an update/adoption path. Legacy runtime stores still require their ordinary provisioning because canonical input/signal/state/timer migration is incomplete.

`journal.GraphPayloadLink` and `journal.GraphOwnedPayload` let SDK callers read exact edges and pass reused payloads to `Append` using public type names. These aliases preserve the underlying receipt schemas; constructing a link never grants ownership. The store still validates the current live graph and the exact source index/receipt. Retiring a graph does not prove application retention/purge ordering; callers must establish that separately. This API makes the verified opt-in component usable from external modules, while full runtime rollout and production online collection remain open.

### Canonical graph terminal client reads

Graph-configured clients now obtain **all** terminal bytes (inline/external successes and failures/cancellation) from a pinned canonical graph terminal entry. `WF_STATE` terminal data is a compatibility mirror, not result authority; a missing mirror or forged result/failure cannot change the graph client's outcome. `GraphStore.OpenTerminal` observes the cursor without acquiring a reader for pending/uninitialized history, and acquires an exact snapshot pin only for a matching live terminal generation. It retains bounded definite-conflict retries and unknown-reply handling from ordinary Open. Terminal reads validate kind/outcome consistency, positive matching invocation, and exact external payload edges/hashes before returning.

`GraphResultPort` and `NewWithGraphJournalPorts` expose production result decisions to native or modeled transports. Await observes invocation identity, reads legacy purge markers, polls graph completion, then rechecks invocation and purge status before returning. Matching/newer purge markers and replaced/retired generations reject; a prior generation's marker does not hide the current graph result. Wrapped metadata CAS exhaustion remains a contention error, never evidence of purge. Uncertain graph operations propagate without automatic reader mutation retry. Pins are released using the bounded cleanup context; unknown acquisitions remain protected until expiry and pruning.

This removes the graph client's dependence on legacy **terminal contents**, not its dependence on the current legacy invocation and retention marker lifecycle. Workers still mirror outcomes into WF_STATE, terminal duplicate probes/reconcilers/snapshots/history readers and purgers still require migration, and production collection remains quiescent. Native read admission/capacity, process/storage crash, scale and full retention/deployment qualification remain open. The new seeded result family covers thirteen canonical-result/lifecycle/uncertainty scenarios with exact trace replay and complete closed-fixture object drain; it does not establish cross-store atomic purge ordering.

### Canonical graph worker terminal duplicate probes

Graph-mode held-lease and owned duplicate probes now use `wf.ReadGraphTerminal`, the same retained terminal validator used by graph clients. A terminal hint still selects a durable probe only; it is never ACK authority. The probe observes current invocation identity, treats legacy state solely as a purge marker fence, opens and validates a canonical terminal snapshot, resolves exact external result edges/hashes, and notifies the parent using the graph entry's original encoded outcome. It rechecks invocation/purge lifecycle after notification before ACK authorization, while holding the reader pin. Unknown reads/acquisitions/notification/cleanup do not authorize ACK, and definite reader metadata conflicts retain the existing bounded retry rules. Missing or forged non-tombstone terminal mirrors cannot select the notification payload or hide a valid graph terminal.

Scheduled current-generation timer duplicates scan the same retained view for cancellation metadata, preserving canceled-timer metrics. Pending graphs, retired/replaced/malformed terminals, unowned payloads and matching/newer purge markers do not authorize this fast path. A prior tombstone does not hide the current generation. Healthy foreign lease bytes/revision and terminal journal contents remain unchanged by terminal probes; owned duplicates still acquire/release their own lease normally. Worker execution continues to mirror terminal state for compatibility, and other readers/purgers still require migration.

The new shared terminal-worker model drives production dispatch and eighteen positive/negative cuts, including withheld ACK on uncertain graph/notification reads, cancellation metrics and post-notification purge fencing. Its negative probes deliberately retain the dispatch message; final graph drain after explicit fixture cleanup is not a claim of successful negative-run delivery or cross-store atomic purge. Native controls cover held and owned duplicate ACKs on R1/R3 with actual large workflow payloads; process/peer crashes, partitions, scale/capacity and deployment remain unqualified here. Parent notification controls use inline outcomes; transfer/retention of external child-result references into a parent's graph remains required before broad graph child/fanout adoption. Production collection remains quiescent.


### Parent ownership of consumed child results

For recorded runtime Call/CallAsync requests, graph signal consumption verifies positive child generation, the invocation's parent relationship and the exact canonical terminal envelope under a child reader pin. It copies external result bytes into the parent's pending payloads and records explicit child identity/generation/ref/hash metadata. The source pin remains held through parent publication and readback. Unknown source/copy/publication failures do not authorize successful delivery ACK. Journal-limit failure entries preserve their child reference declarations too. Replay validates the recorded request and parent-owned envelope without rereading a retired child.

`wf.Context.SetChildResultValidator` installs an optional callback for the exact signal selected by Call or AwaitPromise. Graph workers require its recorded sequence/name/payload to match a durable child declaration before accepting it. This also rejects ordinary signals consumed before a child request, even when their JSON refers to a valid parent-owned blob. Public AwaitSignal remains opaque; a nil callback preserves legacy behavior. The delivery maintains indexed runtime requests/child signals and the exact transformed persisted records.

Native and seeded focused verification is [recorded here](scale/graph-child-transfer-2026-10-08/verification-after-provenance/). Production retention still must preserve the child generation until the parent's ownership publication; reader pins alone do not repair prior retirement. Bounded source-pin cleanup can conservatively leave a pin until TTL expiry after an uncertain release. Other canonical lifecycle/reconciler/purge migration, snapshots/continuations/import/history, native crash/scale/deployment and full release qualification remain required. Production collection stays quiescent.


### Canonical graph repair history

`NewStartScanWithGraphJournal`, `NewSignalScanWithGraphJournal`, `NewTimerScanWithGraphJournal` and `NewSuspendedScanWithGraphJournal` select the workers' same graph store. Their WithGraphJournalPort variants accept modeled discovery/publish ports. `RunRepairLoopWithGraphJournal` uses the existing fenced lease/cursor loop, repair/scan observers and domain clock for these four kinds; it rejects nil journals and unsupported kinds. Legacy constructors continue to select legacy history.

`GraphStore.OpenExisting`/`ReadExisting` never begin/import history. A witnessed uninitialized root returns no history; zero, replaced or retired invocation generations reject, and corrupt/unknown authority or snapshot reads do not become absence. Initialized empty history permits a start repair. The scanners use exact observed invocation sequences, and the signal cache keys history by invocation generation. Fully validated journal metadata is copied under a reader pin and released before making repair decisions; these decisions do not consume external payloads. Unknown cleanup stops the decision unless the protocol establishes successful release by readback.

Terminal timer-hint retirement still deletes only observed hint sequences bound to the terminal generation. Graph unknown/reader-contention/expiry errors retry under the fenced loop without certifying unresolved cursor boundaries; direct stale-generation and corrupt-history errors stop the loop. Prepared model fixtures retain repair wakeups; native start/signal controls execute real workers, while timer history is prepared. This does not migrate legacy invocation/signal discovery, fallback-timer/tombstone state, purge/child retention, snapshots/continuations/import/history/projections/CLI configuration or production online collection.


## Canonical graph purge and child retention

`retention.PurgeGraph` and `PurgeGraphWithPort` select the current invocation. Durable callers use `PurgeGraphInvocation` / `PurgeGraphInvocationWithPort` with an explicit generation; `GraphHandler` records that generation in a Run result before its purge attempts. Register graph retention separately from legacy retention. The graph mode is part of the purge Run input hash.

Purge acquires the target lease and validates the matching canonical terminal and owned payloads. A child can retire when its parent has retired or completed, or when `wf.GraphOwnsChildResult` proves a matching runtime call followed by an exact consumed child declaration with parent-owned terminal/result bytes. Ordinary user JSON pointers and WF_STATE terminal mirrors do not authorize retirement. Parent scans hold a reader and fail closed on uncertainty or expiry.

`GraphStore.FencePurge` persists a terminal-only canonical cursor fence before deleting dependent stores. New opens and appends reject that generation; existing views retain their exact graph. The ordered stages are fence, compatibility marker, signals, legacy journal, generation-bound fallback timers, snapshot, generation-bound native hints, graph retirement, tombstone, purge event, invocation deletion and marker removal. The graph fence and retirement authorize resume after invocation deletion. Broad subject deletion is never repeated on that branch. Explicit old-generation retries reject replacement invocations before deletion.

The optional `purging` cursor field is omitted before use, preserving earlier trace bytes. Older strict adapters reject a fenced cursor; this does not establish mixed-version rollout compatibility. Compatibility INV/state and subject publication still participate in lifecycle coordination. Collection remains an explicit fixture/administrator action; this API does not enable production online GC. Native stage cuts and joined replacements are not process or storage crash qualification.


## Canonical graph client signal decisions

Graph-configured clients use canonical lifecycle for ordinary signals, Cancel, SignalWithStart and generation-bound child notifications. RequireRunning validates a matching terminal entry before reporting ErrNotRunning. Canonical purge/retirement blocks admission; a newer published invocation may precede Begin only when the previous graph generation is retired. Missing-invocation generation checks observe the canonical retirement/high-water metadata, with no WF_STATE fallback.

A duplicate whose source signal has been deleted must match exactly one canonical SignalConsumed record in the observed generation. Name, sequence, payload hash and bytes must match; an external reference requires the record's exact graph-owned edge. Legacy WF_JRN decoys, malformed/doubled consumption and unowned external bytes cannot authorize confirmation. Reader uncertainty/cleanup failures propagate and no wakeup is authorized.

Invocation/lifecycle are rechecked before PublishSignal and again before EnqueueRun. These checks are not an atomic publication fence. An observed replacement or purge after an acknowledged publish returns the committed sequence and an error, without enqueueing; the signal itself remains retained. Start/signal transport publication, incoming external staging, graph-aware worker parent publishers and every remaining lifecycle/import/deployment path still require migration before production GC changes.


## Graph worker parent notification clients

Native and modeled worker constructors bind their client to the selected graph store after option validation. `Client.WithGraphJournal` creates a configuration copy preserving existing transports/observer and the original client. This binding covers terminal execution, replay and duplicate delivery notification; the worker's existing generation-derived child signal key is unchanged.

Canonical parent purge/retirement or observed replacement permits late-notification suppression. A missing parent invocation with no canonical retirement proof remains an error and prevents child ACK. Valid source-deleted duplicate notification requires matching consumed history and owned bytes; corruption/unknown graph metadata prevents ACK. A forged legacy parent tombstone cannot suppress a valid current graph parent. Existing child readers remain held through parent notification and lifecycle rechecks.

Parents and children must share the selected graph runtime lifecycle. Mixed legacy parent rollout, incoming signal publication/staging and atomic publisher/purge fencing remain unqualified. The notification payload can still use legacy incoming signal staging before transfer into parent ownership; production online collection remains disabled.


## Independently indexed forests under one authority

The v4 graphpublication root now holds up to four named append forests beside the original unnamed journal forest. Each has its own record index; all publication, reader, application and retirement operations still share one destination/head CAS. Names qualify exact node/payload intent locations, commit preserves all unselected forests and the collector checks the correct live/pinned forest. Readers capture the complete set and checkpoint it with a versioned fingerprint. Same-forest reuse preserves origin grants; cross-forest transfers currently copy bytes into a fresh grant. All live forests retire together, with old pins remaining protected.

[Complete normal/race/native/100,000-seed evidence](scale/graph-streams-2026-10-08/) qualifies this generic storage primitive. The default graph journal requires v3. The subsequent explicit `CanonicalStarts` mode uses v4 input forests and a versioned runtime cursor; see the Start integration below. Application lifecycle remains caller-validated opaque metadata. Independent indexes do not by themselves provide canonical Start/Signal publication, ordered signal binding or idempotency lookup. A fresh caller must reject fenced lifecycle before preparing; shared-head CAS invalidates previously prepared operations.

### Incoming runtime integration requirements

- Preserve start-once input/parent identity and the original invocation generation contract. A canonical pending start must own its input before any source write; unknown/crashed source publication must be recoverable from its exact durable identity. Begin/dispatch may not infer a ready generation from a compatibility message alone.
- Preserve the original WF_SIG sequence order and per-invocation linearizable queue contract. Canonical signal publication needs a durable reservation and exact source-sequence binding, including crashed/lost-reply writers and competing signallers. Merely appending graph records in whichever CAS order succeeds cannot establish the existing source-order contract.
- Persist pending-operation ownership and scalable key lookup. Permanent idempotency may not be implemented as an ever-growing root map or unbounded history scan. Pending input/signal bytes must belong to exact graph leaves; application JSON pointers cannot grant ownership. Repair must settle an unknown outcome before replacing its logical operation.
- Migrate the runtime cursor, journal append conflict rules, graph views, input/signal/child/cancel clients and workers, all discovery/repair and purge paths together. A concurrent incoming append must not change the logical runtime journal index or permit a stale worker epoch to refresh its fence.
- Isolate/version the source namespace and quiesce incompatible actors before rollout. Old strict adapters must fail closed; unconfigured legacy workers cannot be allowed to execute pending pointer envelopes. Late compatibility writes after retirement may not authorize execution or deletion of a replacement generation.
- Qualify seeded crash boundaries and replay first, then original real concurrent signallers, linearizable Start/Signal histories, native partitions/server/process/storage/power loss, reader capacity/scale, full matrices/24h/million physical drain and deployment/default-adoption/release. This primitive does not discharge those gates or enable production collection.


## Canonical Start input and pending lifecycle

`GraphConfig.CanonicalStarts` / `NativeGraphConfig.CanonicalStarts` enables the v4 input forest and v2 runtime cursor in an isolated or quiesced deployment. Existing v3 generations are rejected; this is not automatic import. `ReserveStart` owns complete input bytes and exact input/parent identity before source publication. A pending cursor has no bound invocation and cannot Begin runtime history. The client publishes a small write-once WF_INV pointer carrying the durable token, hash and parent headers, then binds the actual source sequence before enqueue. Unknown publication requires exact pointer confirmation; unrelated source messages or uploaded objects cannot authorize adoption.

`Client.RecoverStart` reads the owned input and metadata through a retained view after reopening the store and resumes the same operation. This is an explicit recovery API; catalog discovery and a deployed pending-start repair loop are still required. Ready generations still depend on WF_INV source presence for discovery/confirmation. Signal admission rejects pending Starts; incoming Signal staging/order/idempotency has not migrated.

Workers validate the exact bound sequence, pointer token/hash/parent metadata and retained input leaf before effects. Terminal, child-result and graph purge readers validate the same source binding. Retirement clears live input and runtime forests while preserving old reader pins and permanent source/logical high water for replacement. Native object deletion retains attempt tombstones to fence delayed writes; zero chunks does not mean zero metadata messages or qualify permanent metadata capacity.

[Reviewed normal/race, 10,000/1,000 seeded and native R1/R3 evidence](scale/graph-start-2026-10-08/) covers prepared durable recovery cuts, production input execution/replay/purge and exact fixture replay. It does not qualify actual process crashes, all publication/purge races, canonical Signal migration, complete discovery/repair, full current143-family campaign, native scale/partitions/storage/power-loss, original matrix/24h/million physical-drain/default-adoption/release gates or production online collection.
