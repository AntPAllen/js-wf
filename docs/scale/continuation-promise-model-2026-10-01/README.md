# Tier 1 resolved checkpoint promises and uncertain object reads — October 1, 2026

The continuation_promise workload runs production parent and child workers,
client child creation/notification, leases, journal append/read, SDK promise
consumption, frame publication/compaction and fast continuation resume against
seeded transport. The child really produces a 614,402-byte spilled terminal
result. The parent consumes it once, saves explicit Promise locals and outcome
reference/hash in a compact frame, then suspends the next stage on a gate.

A new worker resumes through an archive-denying port. Two awaits of the saved
promise must use one successful verified object read and detached return bytes.
The initial handler must remain at its two prefix entries. Child identity,
outcome reference, frame size, total child calls, request/consumption counts,
terminal result and raw retained-state audit are checked. Both parent and child
must be terminal; the run queue must drain and every queued fault be consumed.

Six modes cover clean execution, one/two transient object-read failures during
resume, lost writes/ACKs of the child's terminal object, and deliberate stored
object corruption. Lost writes cause a second child handler entry but preserve
one child invocation and one consumed notification. Transient resumed reads
retry the same promise with no extra child signal consumption. Corruption is
an explicit rejection control: hash verification must terminate the parent with
ErrCorruptJournal. It is not a clean successful-workflow recovery case.

The ordinary 1,000-seed gate covers all modes, exact replay of the first ten
traces and byte-identical seed-42 generation in two child processes. Seed 42
(terminal drop) and seed 4 (corruption rejection) are pinned and use the common
saved-trace replay dispatcher. The workload runs in the standard sim suite and
extended CI. Extended CI now allows 180 minutes with a 170-minute Go timeout:
the previous broad run took about 104 minutes before these additional slices.
The aggregate normal cluster job also gets 35 minutes with a 30-minute Go
suite timeout, after an earlier suite consumed 18 minutes before the newest
contracts. Per-test fault/recovery bounds retain their existing requirements.

## Evidence and scope

`100k.log` records 100,000 generated schedules, 100,000 choices and 21,160,695
transport events in 735.908 seconds (12m15.9s), maximum virtual time 2 seconds.
All first-seed observations match the deterministic counting helper. There are
83,194 successful recovery schedules and 16,806 expected corruption rejections:

| Mode | Schedules |
| --- | ---: |
| Clean | 16,770 |
| One unavailable resumed read | 16,591 |
| Two unavailable resumed reads | 16,564 |
| Corrupt child: expected rejection | 16,806 |
| Dropped terminal-object write | 16,662 |
| Hidden committed terminal-object ACK | 16,607 |
The seed-count helper uses the exact first scheduler decision for seeds
1..100000; its mode counts are cross-checked against logged first seeds.
Successful recoveries and corruption rejection controls are reported separately.

- `race-corpus.log`: new seeded workload and then-current full pin corpus passed
  under race in 75.563 s. `final-pins-race.log` separately verifies both final
  new pins, including the added corruption pin, under race in 1.220 s.
- `suite.log`: full simulator suite passed in 137.897 s.
- `restore-mutation.log`: compiled SDK overlay drops restored promise outcomes;
  pin 42 leaves the parent suspended instead of completed and fails in 0.024 s.
- `hash-mutation.log`: compiled outcome overlay bypasses result hashing; the
  corruption pin returns a different failure and is rejected in 0.012 s.
- Exact production edits are saved as patches; neither modifies the worktree.
- `vet.log`: simulator vet passed without diagnostics.

This model does not simulate Raft, process death or real disk behavior. The
matching real promise/restart/frame-reference retirement proof is separate.
Integrated seeded promise retirement/GC, multi-fault combinations, promise
SIGKILL/server cuts and remaining continuation gates stay open. Full final-source
matrix/24-hour soak, mixed seed 65 latency and online GC remain open. The original
million-timer campaign continues without restart.
