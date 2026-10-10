# Canonical Signal binding budget model

The failed [compiled PostgreSQL campaign](../graph-compiled-canonical-postgres-2026-10-10/README.md)
received a source acknowledgment and replies to all captured requests, yet
returned an unknown outcome during binding. This model isolates the client's
existing three-second aggregate binding context from server behavior.

The production client and journal execute through the seeded in-memory graph
and Signal transports. After ordered source discovery, a wrapper projects a
deterministic owned-body cost onto the bounded binding operation. It explicitly
checks that the production context has a deadline no greater than three seconds.
This is a cost model, not a replay of physical chunk timing or Raft.

- A 2.8-second modeled cost succeeds.
- A 3.2-second cost returns `ErrSignalUnknown` wrapping `DeadlineExceeded`.
  The reservation and acknowledged source remain; no queue binding or wakeup
  is published. A later same-token recovery binds exactly once after healing.
- A corrupted body also prevents binding and remains recoverable after healing.
- Every mode executes 16 seeds and byte-identical trace replay comparisons.
- A separate control waits for the **actual** production context deadline once
  and proves recovery. The seeded matrix uses virtual time without sleeps.
- Independent raw reference checks, exact body/identity/sequence checks, one
  source and one additional run message guard every recovered result.

[Development source hashes](development-source.json),
[actual command receipt](development-command.json), and
[race log](development-race.log) record the executed worktree check. The focused
race run passes 22.742s, including all 112 existing publication-recovery cuts.
These development receipts do not establish clean-source or full-suite
qualification. The operator CI client row includes both new tests.

The model establishes that individually healthy reads can exceed an aggregate
binding window and safely return an unknown outcome. It does not establish the
exact internal cancellation point or a NATS defect in the failed native run.
No runtime deadline, fixture bound, payload verification or cross-forest
ownership rule changed. The PostgreSQL CLI campaign remains failed. A resource
isolated native qualification and the original larger gates remain open.

## Clean frozen-source qualification

Frozen `d26f571` now independently passes the focused race qualification under
an explicit one-core service quota, GOMAXPROCS=2 and GOMEMLIMIT=1GiB.
[Independent review](qualification/review.json) verifies 1,925 complete
Git/before/after inputs, actual compiler/test exits, matching retained terminal
service, and the retained race executable's SHA/build flags. All 48 cases and
exact replays, the real deadline control and 112 existing recovery cuts pass.
[Supervisor and independent reviewer](qualification/) preserve reproducible
commands and raw logs. This closes the focused source qualification, not the
failed native PostgreSQL CLI or full implementation gates.
