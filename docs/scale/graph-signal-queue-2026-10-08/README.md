# Canonical Signal publication, binding and worker intake

This follows the frozen owned-reservation component at `691434f`. The opt-in canonical Signal path now publishes bounded source pointers, binds a canonical ordered queue, copies consumed bodies into journal ownership, and replays through production workers. Frozen source qualification is pending.

## Protocol

`BindNextSignal` asks its source port for the first matching retained WF_SIG message at or after the confirmed frontier. It validates the current-generation pointer against the exact retained reservation and owned body. One root CAS publishes the queue descriptor, owned body, bounded bound-key index and source frontier. Duplicate pointers advance only the frontier and retain the first operation/sequence. Referenced/uncertain reads and forged current pointers do not advance it. Retired older generations may be skipped using the root's previous-invocation proof; future generations stop binding. Concurrent binders must reobserve the frontier, population and lifecycle after releasing their snapshot.

The existing runtime cursor v3 gains optional binding population/frontier fields and the `signal-queue` forest. Old strict readers reject populated unknown fields/forests; default modes omit all new zero fields. Retirement clears live input, queue and journal ownership together; acquired readers preserve their exact old-generation snapshots. Replacement resets both Signal populations/frontier.

The client requires the additional `CanonicalSignalPort` source-order operations. It holds the owned input pinned across pointer publication, uses a request-hash/token message ID, binds preceding sources before returning success, and confirms uncertain outcomes through an exact canonical binding. A bound duplicate returns the original sequence even after source purge and native dedup expiry. `RecoverSignal` accepts a request and optional captured token, requires no caller payload and never reserves a replacement. Queue binding is limited to 256 cuts per call so partial progress is durable and resumable. Enqueue uncertainty remains explicit.

Workers can now use this mode. Intake repairs existing published-but-unbound sources, validates queue operation identity, and copies every consumed body into a journal-owned payload. Consumption records contain the queue index/token. Replay checks those locators against the canonical queue and validates owned journal bytes; it never fetches the source body or legacy WF_BLOB. Cancellation uses the same queue and is applied before handler entry. Existing child provenance validation and ownership transfer remain required. Snapshots/continuation import are still rejected.

## Development evidence

Preserved JSON logs cover reverse reservation/source order, 32 indexed operations across 16 seeds, duplicate source pointers and complete source purge, unknown/forged earlier reads, concurrent binding, retirement, retained old-generation reads after replacement and future-generation fencing. Client controls exercise seven cuts across 16 seeds: healthy, source dropped, source ack lost, binding dropped, binding ack/readback lost, enqueue dropped and enqueue ack lost. Exact recovery, changed-body rejection, purged-source/expired-dedup duplicates, captured-token fencing and absence of legacy blob writes are checked.

Native R1/R3 client fixtures confirm actual Start source identity, publish/recover in reverse reservation order, retain a 5 MiB input, reopen adapters, return original sequences after source deletion, preserve held queue reads through retirement and completely drain physical objects/chunk subjects after releasing readers.

Native worker fixtures consume a 5 MiB queued body after source deletion, suspend/reopen worker instances and adapters, complete with one effect and exactly two consumptions, and apply queued cancellation before handler entry. Six hostile consumption metadata substitutions reject while the healthy replay succeeds. Reused native child fixtures run synchronous/asynchronous and inline/external-result cases in both R1/R3 with the new mode, including parent ownership transfer, source/child purge and parent replay. These are in-process native fixtures and worker-instance restarts, not OS process kills or a full native fault matrix.

Initial native DeleteMsg signature and missing time-import build failures are preserved with failed verdicts. Shared inventory remains 144 workloads/710 existing pins; the complete new flow has not yet joined a shared seeded/replayed workload.

## Remaining work

Automatic catalog discovery/recovery of reserved incoming Signals, recovery when their enqueue/source disappears, source-order concurrency and purge histories, complete shared seeded pipeline coverage and capacity/liveness qualification remain required. Explicit `RecoverSignal` covers the durable request/token cut; it does not prove autonomous catalog repair.

Runtime state, timers/tombstones, snapshots, continuation/import, invocation discovery/CLI/deployment migration, full extended simulation, original native matrices, actual 24-hour soak, million-item physical drain, dependency/default adoption and release gates remain open. Production online GC remains disabled. No original target or historical verdict is changed.
