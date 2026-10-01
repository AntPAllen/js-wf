# Mixed recovery placement diagnostics — October 1, 2026

Hosted seed 12 at 7e414b2 failed terminal p99 at 40.574 seconds, with two signal
invocations spending measured 39.293/38.436 seconds in 48 pre-append lease updates
each. The raw failing evidence remains in the separate seed-12 proof. Existing
source renewal/fencing semantics have not changed.

## Experiments and limits

An unmodified local seed-12 run at 87d489a passed in 34.062 seconds with terminal
p99 7.607 seconds. A compiled Go-overlay prototype adds a read before lease Create
and returns ErrHeld only for a well-formed, initialized lease less than one second
old (nonnegative age). It does not skip renewals or change fencing. Its lease
suite passed in 3.489 seconds, but its mixed run took 46.709 seconds with terminal
p99 14.949 seconds. It has NOT been promoted to production.

Those numbers are not a controlled performance comparison. The concrete saved
schedules differ: baseline delays node 0, isolates/kills node 1 and pauses node 2;
the prototype delays node 1, isolates/kills node 2 and pauses node 0. Hosted seed
12 delayed node 2, isolated/killed node 0 and paused node 1. All have 85 ms delay,
18 ms pause and 31 ms kill. Targets derive from the actual journal leader at
fixture startup, so the same seed need not select the same physical nodes.
Pre-fault progress also varies. On the baseline, replacement signal handlers
needed 0/10 renewals for mixed-02-0/mixed-06-0; the prototype needed 49/40. Comparing
aggregate latencies cannot establish a causal benefit or regression here.

The existing disk summarizer joins interleaved syscall starts/resumes, preserves
thread/path/timing and labels Raft groups from pre-fault JSz. Applied to the hosted
trace, it finds 3,719 delayed calls, ten unfinished calls and zero unmatched lines.
Lease stream and lease Raft groups account for 283/497 calls respectively. This
is overlap/attribution to storage groups, not attribution to individual RPCs.
Concurrent syscall-duration sums do not equal elapsed client delay.

The tracer already selects existing JetStream file paths with strace -P; the
trace does not justify treating this as unfiltered network-write injection.
The [strace documentation](https://github.com/strace/strace/blob/master/doc/strace.1.in)
describes combining path filters with injection. Newly created files remain
outside this injection until it is reapplied, as documented in the fixture.

## Fixture change

Mixed runs with FAULT_SCHEDULE_OUT now save independent monitoring snapshots
immediately after confirmed journal heal, ten seconds later if recovery is still
active, and at final teardown. Recovery snapshots run in a separate goroutine;
the main path does not wait before enabling work or change its heal timestamp.
The goroutine joins on every exit. HTTP calls have two-second bounds. A dead or
unresponsive node produces a timestamped diagnostic_error JSON artifact instead
of silently missing evidence. File-write failures fail the fixture. Final snapshots
are captured before server teardown, including failed tests.

Each successful JSz response includes its own server timestamp and reported
stream/consumer leaders. The snapshots describe placement at those times. They
do not identify the RPC that caused a wait and do not guarantee that leadership
remained constant between captures.

The final fixture passed seed 12 under race in 50.914 seconds, terminal p99 19.520
seconds, all 28 invocations terminal with passing history/integrity/drain gates.
All three phases were retained. The during-recovery capture reports journal,
lease and run leaders on node 1 at its timestamp; node 2 was unavailable and its
error artifact is present. This final schedule matches the prototype's fault
node choices, but is still a separate uncontrolled timing run.

## Files and remaining work

- baseline/, prototype/: raw logs, actual schedules, operations and available
  disk/pre-fault snapshots; neither clears the failed hosted gate.
- final/: full race run and before/after-heal/during-recovery/final monitoring,
  operation and disk artifacts.
- hosted-seed12-disk-summary.json: existing parser output over retained CI data.
- unpromoted-acquire-prototype.patch, prototype-lease-tests.log: experimental
  overlay and its unit evidence; no production acquisition change.
- SOURCE_SHA256SUMS, vet.log: final fixture/helper sources; vet exited zero.

A matched placement/progress API-level contract remains needed before promoting
an acquisition optimization. Server cause remains unconfirmed. Seeds 2, 12 and
65, final-source full Tier 2 matrix/24-hour soak, and other implementation-plan
gates remain open. Original full Tier 1 and million-timer campaigns continue
without restart. Standard CI at 7e414b2 has now passed.
