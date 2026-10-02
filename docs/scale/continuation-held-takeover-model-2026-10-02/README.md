# Continuation limit with a held lease and retained-state restart

The 120th seeded workload, `continuation_limit_held_takeover`, runs the production
worker, SDK, journal, lease and staged replay code. Its 27 combinations are three
cuts (after SignalConsumed, StepCompleted or limit Failed), budgets 16/18/20 and
virtual cluster heal delays 0/500/2000 ms. Two production SDK continuation frames
establish a nonzero index and step offset before each cut.

A journal adapter stops the actor after the selected append commits. The model
suppresses that dead actor's deferred cleanup, leaving its actual modeled KV
lease untouched. This is an actor-lifetime assumption, not a hypothesized
successful server delete reply. Committed journal, checkpoint, signal, outcome
and dispatch state survive the modeled outage. Fresh adapters and a fresh
production worker recover them after the lease expires. Production Acquire must
return ErrHeld one millisecond before expiry; a stale revision cannot update
the lease after takeover. Nonterminal cuts require a higher replacement epoch.

All final prefixes remain identical, journals contain exactly the budgeted
entries, effects stay zero, checkpoint suffix recovery avoids archive reads,
offline replay consumes both continuation stages, terminal state matches the
journal and raw integrity/dispatch-drain checks pass. Recovery must stay below
30 virtual seconds with production TTL12s. This models committed-state retention
and lease timing; it does not simulate NATS disk persistence, Raft or OS SIGKILL.

The focused 1,000-seed race run covers all 27 combinations, passes the unchanged
old limit workload and all 265 earlier pins, and completes in 46.751 seconds.
Exact first-ten replay and two-process generation identity pass. The new seed42
pin separately passes generic disk replay. The actual compiled production
suffix-budget guard mutation executes one forbidden effect after takeover and
fails the intended check in 0.038 seconds. Vet passes.

The initial disk-replay command used a relative path from the wrong test working
directory; its file-not-found failure remains rejected and preserved. The final
helper saves failed traces before reporting errors, a change made after the
focused race run. Exact final-source full-suite qualification remains open.

All 20 evidence files are losslessly archived with SHA256 readback. Original
logs, positive/negative traces, overlay, source-at-archive hashes and new pin
remain in `/tmp/js-wf-continuation-held-model-20261002`. This fixture adds coverage;
it does not clear the current 120-workload 100k release gate or replace real
combined-cap recovery evidence.
