# Bounded retained key index

Frozen `45d355f` passes full-package normal (5.338s) and race (22.804s) commands, six top-level groups each, with count1, a5-minute gate, GOMAXPROCS2 and512MiB. Selected source inputs match Git and remain unchanged. [Qualification and review](qualification/).

The persistent crit-bit codec packs a changed search path into one same-forest append record. Keys are256 bits; values are64-bit caller-defined identities. There are at most257 copied nodes and20,576 packet bytes, independent of population. Locators are record/slot positions with strictly backward edges, not object references. Reads cache at most257 bounded packets; resolving each packet through an append forest also incurs that forest's bounded ancestry reads. This is not a throughput or capacity result.

The reader adapter derives population from a captured named forest and validates its authoritative pin even for empty lookups and no-op updates. A copied forest root cannot create ownership; released/expired or uncertain snapshots cannot prove absence. Update returns prepared metadata bytes only. Callers must validate their lifecycle, exact source population and observed head when appending, and establish payload ownership with the graph protocol. Reader calls must finish before pin expiry (or renew first), using the same collection clock and suitable context deadlines.

Thirty-two deterministic map cases perform512 updates each, compare captured populations against independent maps, and verify identical updates do not grow storage. A constructed256-bit search path exercises the full257-node packet bound. Corrupt schema, padding, child locators, prefix/value, alias/cycle/future/out-of-range shapes, cancellation and uncertain reads reject.

The owned model checks captured population stability, an unpublished competing fork, unknown reads, retained retirement, released/expired pins including an empty forest, and complete fixture object drain. Native R1/R3 reopen adapters after16 appends and retirement using the exact retained checkpoint: eight captured keys resolve to their owned input payloads, eight later keys remain absent, and release rejects further reads. Fixture collection verifies zero objects plus a complete native subject census containing zero physical chunks. Permanent metadata may remain. Collection clock advances are explicit fixture controls, not server clock faults or production GC.

This supplies the bounded lookup primitive needed by canonical Signal idempotency. It does not implement Signal reservation descriptors, publication, source-order binding, queue intake/drain, generation lifecycle migration, a shared simulation workload/corpus, native concurrent publishers, process/storage faults or scale. The existing codec reader interface allows the eventual Signal reader to extract a bounded packet from its validated owned record envelope. Runtime adoption and all original plan acceptance gates remain open.


## Integrated main verification

The unchanged index code also passes full-package normal/race at frozen main `e6c3c4041da188247d1bef84898fb221d8678676`, including the completion retry and CI integration changes. [Merged-source evidence](integrated-qualification/). The original isolated source revisions are retained as main-history merge parents so their committed input checks remain reproducible from a clone.
