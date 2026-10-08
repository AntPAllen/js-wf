# Experimental native graph object Port

The graph prototype now implements its complete `Port` with a dedicated graph authority and graph object bucket. This advances native graph adoption; it is not wired to canonical workflow runtime storage or production online GC.

## Object contract

Provision a new bucket using the graph package's `NativeObjectStreamConfig`, then explicitly open it. Its `js-wf-blob-format=graph-recoverable-v1` marker is required on every operation. The direct-reference native adapter rejects it before enumeration; the graph adapter rejects direct buckets. Admission never creates missing storage. Bucket ownership, administrative privileges and deployed permission policies remain separate requirements.

Physical names encode exact content hash, scoped generation, publication owner and unique upload ID. Put checks a witnessed uploading scope and its registered grant before writing bytes. A conditional sequence-zero metadata reservation permanently marks the attempt `staging` before any chunks. A retry or concurrent duplicate name cannot acquire that reservation. Chunks use synchronous acknowledged 128 KiB writes. Completion publishes metadata conditional on the exact reservation sequence; no uncertain acknowledgment is treated as success. Staging attempts are enumerated even before chunks exist. They cannot be read as complete objects.

Deletion first acknowledges a permanent `deleted` metadata tombstone, then purges only that attempt's chunk subject. Late reservations/completions fail their sequence condition. Late chunks remain visible to subsequent collection. Attempt metadata and scoped authority high-water records are retained. Missing Delete is safe; unknown/corrupt subjects or metadata block enumeration. Tombstone/authority metadata growth and large census performance still require scale qualification.

Get validates the exact receipt, complete bounded metadata and caller's byte budget, checks physical chunk count, then uses context-bound native GetMsg requests with an exact subject and monotonically increasing physical sequence. Each chunk size, total bytes and content SHA are checked. It creates no consumer or blocking SDK ObjectResult reader. Native message reads remain physical integrity checks; reader retention/ownership pins are not implemented.

## Controls and scope

R1/R3 tests use actual native graph Port operations to append 17 records sharing one payload, sweep after every append, check an independent raw JSON child census and one payload object, retire the root and verify zero live objects and zero physical chunk subjects. Ordinary ObjectStore payload reads remain compatible. Another eight replica/fault combinations execute native writes while withholding reservation, chunk or completion acknowledgments, or collecting immediately before completion. Attempts cannot be reused; late chunks after tombstones are discovered and purged.

Original-head publication versus collection is controlled at the actual native root write. The collector wins, prevents the prepared commit, and permits a fresh append at the advanced head. A delayed delete of the old payload cannot remove a newly owned same-content payload. Read controls reject altered content, subject or sequence, missing chunks, invalid budgets, canceled reads and opaque chunk subjects; a controlled native GetMsg barrier verifies cancellation of an active chunk read. Format isolation and missing-bucket admission are checked.

These are component regressions using embedded servers and controlled transport wrappers. They are not real route partitions, deployed permissions, process/power loss, native history linearizability, 100k/1M object scale or complete campaign qualification. Runtime migration, reader pins, partial compaction/import, original native matrices/24h/million physical drain/default dependency adoption/release gates remain open. Precursor logs are retained with their earlier source scope; full frozen-source normal/race regression is pending at this commit.

## Frozen regression result

At `0c008930ddfb99300b85c32f0cd3428ee49a7761`, all 27 top-level graph publication package groups pass, normal20.834s/race77.039s, count1/5m/twoGoCPU/512MiB. All 63 selected source/module/cluster inputs match frozen Git and remain unchanged after both closed runs. This is standard local component regression; it does not provide actual SDK process/dependency admission or complete native release qualification.

The final delayed-delete control deletes the old payload receipt, exercising newly owned same-content payload isolation. Earlier expanded controls deleted an old node receipt and retain that narrower scope in the precursor log. No precursor is relabeled as final acceptance.

The original full126 normal100k SDK2827904/user service remains confirmed live at `074bcfc` under its unchanged300m budget; no restart. It predates this graph Port and has no final acceptance yet.
