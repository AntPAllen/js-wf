# Graph worker parent notification development — 2026-10-08

Both native and modeled worker constructors now bind their client to the selected graph store. Completion, terminal replay and held/owned duplicate paths therefore use canonical parent admission, retirement and consumed-signal confirmation. `Client.WithGraphJournal` copies configuration, transports and observer; it does not change the caller's original client. An adapter supplies result reads/waits for previously unbound clients.

Development passes22 native R1/R3 cases (4.919s) covering active/uninitialized parents, forged compatibility mirrors, canonical purge/retirement, missing retired invocation, ID reuse, unconfirmed missing invocation, and inline/external/corrupt source-deleted duplicate notification. No handler/effect executes; a healthy held child lease is unchanged. Native constructors select the graph client without injecting one.

The new shared family passes1,000 exact-replay schedules across12 modes. `model-initial.jsonl` (8.145s) and `model-after-fixture-cleanup.jsonl` (8.390s) are successful successive development fixtures; the finalized fixture publishes the canonical child terminal before preparing an earlier notification. Parent uncertainty and corrupt/unconfirmed consumption NAK; canonical retirement/replacement suppresses a late signal and ACKs; valid consumed bytes confirm without republishing. Graph drain uses explicit fixture retirement/expiry, and legacy work remains retained.

`native-initial.jsonl` preserves a draft build failure: the test called a nonexistent Lease.Inspect method. The corrected test reads the actual WF_LEASE KV revision and bytes. `client-binding.jsonl` passes (0.006s), proving graph binding leaves the original legacy behavior intact, uses supplied transports, supports canonical Await and rejects incomplete configurations.

## Existing regression migration

`corpus-before-pin-migration.jsonl` rejects exactly three old graph terminal-worker parent traces. Constructor binding replaces their expected legacy parent-state reads with canonical lifecycle and invocation rechecks. `terminal-worker-rebase.jsonl` passes1,000 old-family schedules (1.728s). Seeds4/20/15, workload, decision lists, injected dropped/lost replies and ACK/NAK assertions are unchanged. [Old/new SHA-256 and source lineage](pin-migration.json) retain the originals at Git350a447. Every other639 prior fixture is byte-for-byte unchanged.

`model-final-pins.jsonl` passes1,000 finalized new-family schedules and all642 previous fixtures after that documented migration (9.216s). Twelve new pins extend source inventory to141 workloads/654 pins. Committed-source normal/race and extended verification follow; these mutable development checks are not acceptance.

## Limits

Read rechecks remain separate from native signal publication; this does not make publishing atomic with purge. Incoming start/signal staging/publication, state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment remain incomplete. Graph worker mode requires the same graph namespace/lifecycle for parents and children; mixed legacy-parent migration is not qualified. Prepared parent/child terminal histories and joined workers do not prove process/storage crashes or whole-queue drain. Full current141 simulation and all original native capacity/partition/crash/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent.
