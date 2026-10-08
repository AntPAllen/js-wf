# Native authority read witnesses — 2026-10-08

`NativeAuthority` previously returned administrative message GET results directly. The locally inspected NATS2.15.0 GET handler checks leadership and reads storage without an explicit quorum read barrier. That source inspection does not establish a demonstrated upstream read defect. The publication protocol requires linearizable authority reads, so the adapter now confirms each observed value with a conditional publication on the same subject and waits for its acknowledgment before returning.

## Contract and migration

A witness preserves the root head, token, blob graph, data, or fence revision/generation exactly. It advances only the physical stream sequence. A stale or missing snapshot fails the actual expected-last-subject-sequence condition; the reader obtains a fresh snapshot and retries, at most16 attempts inside its original context. An ambiguous/lost witness reply is an error, never upgraded by another GET. Successful logical writes still advance their head/revision by one. Their own conditional write supplies the quorum acknowledgment, so their preliminary snapshot read does not need a separate witness.

Absent identities acquire a permanent revision-zero record with no root/fence content. First logical publication still starts at head/revision/generation one. Validated legacy v1 records are rewritten as v2 witnesses at the same logical revision; native tests preserve a legacy head7. Quiesce old readers and writers before migration: older adapters reject v2 records, and mixed-version execution is not qualified. Never delete/recreate the protected stream or reset its high-water marks to upgrade. Reads now need publish permission and generate durable writes; physical sequence churn, read contention and extra absence metadata have scale/operational implications still to qualify.

Publication fault fixtures now exempt the explicit witness header when targeting state-changing packets. Only actual HPUB headers are inspected; payload-only header imitations do not bypass the gate. Existing paused snapshot and uploader/collector SIGKILL cases still hold their original logical publication/chunk/purge boundaries.

## Evidence at frozen4178f50

Actual retained race SDKs: journal2753038 (22.1646s, Snapshot|Checkpoint), blob2753137 (43.4733s, entire package), proxy2753290 (1.1223s, four named selectors). Each uses2GoCPU/512MiB/count1/3m; native authority fixture parents remain20s, object/new witness cases30s. Complete source-before/after inventories bind2406 inputs, actual executable/birth/argv/environment profiles, module versions and full retained fixture bytes.

Independent reviews accept13 existing snapshot graph/anchor/import/pause proofs,20 existing blob/object/SIGKILL proofs, and five new R1/R3 witness cases:

- Controlled stale value, stale absence, fabricated future and persistently stale GET replies require real conditional CAS acknowledgment. Persistent stale reads reject after16 attempts. Blob phase/generation reads follow the same rule.
- Lost witness acknowledgment after actual native commitment fails closed; cancellation before GET is honored.
- An actual held old witness is completely forwarded after a committed replacement. Native error10071 rejects it; the successful read returns the replacement without changing its logical head.
- A real R3 route minority yields no successful read, the majority commits head2, and the healed peer reads that head under the original30s parent.
- Native v1 upgrade preserves head7 and writes a v2 envelope.

406 altered-proof controls reject;14 blob wire entrypoint INFO identities bind to retained native peer identities, and the two read-witness wire transcripts also bind INFO to their peers. The initial compile/helper naming failures and two old-review assumption failures are retained. Reviewer corrections did not rerun the successful frozen native bodies. `accepted/blob-component/archive-verification.json` and its inventory/receipt cover the complete combined root, including all three actual binaries, source, native stores, wire and raw logs.

## Remaining requirements

This qualifies an experimental component. Actual NATS-server OS/power loss, all arbitrary partitions/clock faults, naturally lost root/witness responses, concurrent runtime/scale, privileged lifecycle permissions, large/hierarchical object graphs, all invocation/signal/live journal/state/result reference migration and production online GC remain open. External Go module file contents are not separately captured; embedded module/application version identities are bound. Previous full Tier1 source scopes do not qualify this newer adapter: the full125 race and original full124 normal campaigns continue independently at their recorded frozen sources. Original matrices/actual24h/million physical drain/dependency adoption/full release also remain open.
