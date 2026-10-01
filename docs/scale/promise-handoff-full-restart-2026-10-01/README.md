# Frame-held promise handoff cuts with full server restart

The resolved-promise process fixture adds after_suspended and after_handoff_release
cuts. A concurrent-safe atomic flag activates the observer only after a parent
continuation frame is stored, so the initial parent suspension waiting for its
child and child-worker releases cannot be mistaken for handoff cuts.

Before killing anything, after_suspended requires the exact continuation wait,
a retained parent lease and no continuation handoff. After handoff/release must
have exactly one generation/checkpoint-specific run message and no parent lease.
Both cases verify worker SIGKILL and kill all three servers before any restart.
All original prefix, fencing, raw state, immutable result, detached promise,
archive-free resume, retired-child GC and offline replay gates still apply.
These checks do not prove reply-loss semantics for release or enqueue.

The two handoff cases pass under race in 37.370 seconds. A final race run of all
eight publication/handoff cases passes in 144.645 seconds. Every raw kill-to-
terminal and enabling-signal-to-terminal delay stays under 30 seconds; exact
per-case timings are in the retained log. Vet passes.

A compiled omitted continuation enqueue fails the required handoff message
checker in 5.320 seconds. A compiled omitted real lease release fails on a
retained parent lease in 20.626 seconds. Its first unused-variable build failure
is retained and explicitly excluded as a mutation control; the final overlay
compiles and fails semantically. Source hashes and reproducible patches are kept.

## CI and campaigns

The remote main branch contains the previous dedicated workflow, but GitHub's
workflow registry returns 404 for it and records no push runs for fcf4537. The
cause is unconfirmed. To make this gate callable through an existing registered
entry point, the eight-cut job is now in test.yml and the unregistered standalone
file is removed. The standard workflow supports manual dispatch on main; its
hosted eight-cut confirmation remains pending. No repository setting is changed.

The fcf4537 standard run 36834846249 and 20-seed mixed run 36834849592 were
manually started and remain in progress at this write. Fresh 200-seed mixed
run 36835269534 is also running on fcf4537, whose production runtime/model match
the latest worktree. This is one combined fault generator, not the whole matrix.
The 844db83 20-seed mixed and paired CAS throughput workflows completed cleanly;
the later full Tier 1 jobs and original million-timer process remain live.

Final shared-worker-promise checker source passes 100,000 schedules in
1,164.872 seconds with 36,961,056 transport events and maximum virtual time
1,000 ms. Its source-manifest comparison and log are in the sibling promise-
retirement proof. This response-fault model does not simulate physical NATS
persistence. Full matrix/24-hour soak, remaining continuation combinations,
online GC, TTL and capacity acceptance remain open.
