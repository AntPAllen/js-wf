# Copied audit physical evidence correction

Historical `queue_all_three_peers` records are three leader-routed Stream.Info API
views, even though the clients are pinned. They establish logical queue drain and
64 durable consumers, not three local physical store states. Earlier copied-audit
prose calling these physical peer checks was too strong. Original raw evidence and
native outcomes remain preserved. Native rolling-upgrade pinned /jsz snapshots
already establish actual physical peer states and are unaffected.

The copied helper now additionally reads each server's public pinned /jsz endpoint,
requiring distinct server IDs, exactly one local WF_RUN per server, zero messages,
and 64 consumers. Full snapshots and observation intervals are retained; reads are
sequential, not simultaneous, and share the unchanged original 20-second integrity
and drain budget. No provisioning, worker, acknowledgement or purge is added.

The reusable closed-copy reviewer binds executable, runtime/module source, executed
helper/producer/observer, original files, complete history/cohort and optional
canonical pre-copy verifier. `--require-physical-peers` rejects legacy API-only
results; optional legacy review explicitly returns physical verification false.

Four controls accept new physical and legacy API evidence separately, rejecting
10 cohort/budget/API and 14 physical identity/state/timestamp corruptions. The
helper compiles from a copied .go template. The accepted upgrade closed fixture
rechecks without starting NATS; its local physical copied coverage is false.
Fresh positive-clock copied execution is the next runtime check.
