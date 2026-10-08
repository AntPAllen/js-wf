# Canonical graph-owned Signal inputs

The description below records the frozen reservation-only component at `691434f`. Subsequent [publication, ordered queue binding and worker intake](../graph-signal-queue-2026-10-08/) supersede its client/worker guards; original frozen evidence and verdicts remain unchanged.

This component implements the first durable Signal cut: one root CAS publishes the immutable request descriptor, owned body, and persistent bounded idempotency index together. It does not publish or bind WF_SIG, enqueue a delivery, or integrate worker queue intake.

## Storage and admission

`GraphConfig.CanonicalSignals` and its native counterpart require `CanonicalStarts`. The new runtime cursor v3 rejects old readers and stores the authoritative `signal_inputs` population. Default modes retain their existing schemas and omit the new zero field. Each `signal-input` forest record contains one immutable descriptor, one bounded retained index packet, and exactly one owned body edge. The key hashes the canonical type, ID, invocation, signal name and idempotency key tuple; index zero is valid.

`ReserveSignal` validates live generation and optional running admission, looks up the exact indexed request through a retained reader, and validates the complete body before returning a duplicate. A changed body rejects. Missing keys prepare an index extension against the captured population, release the reader, reobserve admission/population and publish against the exact fresh head. Definite conflicts require a fresh call; uncertain outcomes require an exact retained reservation read, not staged object presence.

`ReadSignalInput` and `GraphView.SignalInput` validate exact descriptors, body ownership, hash/size and pin lifetime. Referenced read failures are errors, never witnessed absence. Acquired readers can retain old inputs through retirement; new readers reject retired or replaced invocations. Replacement resets the Signal forest and population.

The client rejects Signal publication and worker configuration rejects queue intake when this new mode is enabled. Those explicit guards remain until ordered source binding, recovery and queue consumption are implemented. Default legacy Signal behavior is unchanged.

## Evidence

Development logs are preserved under `development/`, including the receiver-name build error, an invalid retirement fixture that attempted to retire nonterminal history, and a native fixture build error. These failures retain their failed verdicts. Successful development controls cover indexed reopen/duplicate/mismatch, terminal admission, replacement, held readers, unknown commit/read failures, competing same/different key publishers, retirement at publication, old-schema rejection and pin expiry.

Native R1/R3 fixtures stage a 5 MiB input, reopen adapters, validate duplicates, retain the body after retirement and collection, replace the invocation and completely drain objects and physical chunk subjects after releasing the reader. Their invocation sequence is fixture-bound with `BindStart`; they do not prove native Start source confirmation, native Signal publication, crash recovery, production online collection or server fault behavior.

Frozen source `691434f221d3b6e2825849a7794c06fff7daa2c2` passes all six commands under two Go CPUs, 512 MiB Go memory limit, count one and five-minute package deadlines. Each mode passes 23 journal groups, seven client groups, three worker groups, and the full 710-pin corpus. Normal journal/client-worker/pins commands take 12.700/9.600/2.238 seconds; race commands take 104.024/43.471/33.712 seconds.

The separate executed source/trace review verifies all 1,592 selected Go/config/corpus inputs against that Git commit and unchanged before/after, regenerates the exact selected group sets, checks every package terminal result and every one of the 710 pin names, and verifies both native replica cases and physical drain output. No selected test skips or data races occur. Complete commands, JSON events, inventories and review are under `qualification/`. The shared simulation inventory remains 144 workloads and 710 existing pins; these direct deterministic production-API cuts do not add a shared seeded family.

## Next integration

1. Publish a bounded pointer only after owning the exact reservation.
2. Discover and bind source messages in actual WF_SIG sequence order; unresolved earlier reads must stop the frontier.
3. Persist a canonical queue and bounded bound-key index, with explicit body ownership through retirement.
4. Drain through production workers and recover reserved/published/bound/enqueued cuts without duplicating operations or adopting another invocation.
5. Add the complete flow to the shared seeded transport, pinned replay, native concurrent source/generation histories and fault gates.

Runtime state, snapshots, continuation/import, discovery/CLI/deployment migration, full extended simulation, original native matrices, actual 24 hours, million-item physical drain, dependency/default adoption and release gates remain open. Production online GC remains disabled.
