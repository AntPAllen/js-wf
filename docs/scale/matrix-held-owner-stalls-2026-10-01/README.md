# Live-owner stalls in sustained journal-leader seeds

The prior-source whole-matrix campaign 36850800757 at 0c564d6 fails journal
seed 1 on parent seed-1-batch-82-8 and seed 13 on timer seed-13-batch-81-0 after
about 815 seconds. Both passed batch-80 raw audits with 2,240 terminals, but
neither completes its final cohort. Full raw logs and per-seed artifact ZIPs
remain in ../full-matrix-campaign-2026-10-01. held-owner-timelines.json derives
active delivery durations directly from the retained dispatch events and records
the source dispatch SHA256s.

Seed 1's parent acquires/releases/ACKs five deliveries, the last at 10:52:04.596Z,
but three grandchildren acquire near 10:51:41.8Z and only report execution_retry
context canceled near 10:56:41.7Z. Seed 13's timer and two short workflows acquire
near 10:51:53.23Z and only report context-canceled retry near 10:56:53.23Z. Timer
retries fetch 110 deliveries and find ErrHeld 109 times over that interval.
These are owners whose execution remains open for approximately five minutes,
not the already-terminal duplicate backlog. No final raw journal/KV census,
operation timing or pre-cancellation stack exists for those original failures,
so the blocking call, final state and server cause remain unconfirmed. The
absence of a terminal result is not itself proof of an invariant violation.

## Additional failure evidence

Sustained batches now capture goroutine stacks before canceling workers when a
batch returns an error. They collect the current parent cohort plus owners from
dispatch history, keyed by delivery identity so a losing duplicate NAK does not
hide an owner. Up to 256 targets are inspected by 16 bounded readers under one
fresh 15-second context, with two-second API attempts. Raw last invocation,
last journal record, immutable outcome and lease value/revision/created time
are retained; missing/transient/canceled reads and target truncation are explicit.
The existing bounded queue/consumer/raw-message diagnostic runs afterwards.
The stack has an 8MiB bound and warns on possible truncation. These are diagnostic
readbacks, not atomic snapshots, independent full-journal audits or repair writes.

WF_MATRIX_OPERATION_TIMINGS=1 records production per-call finish timings for
in-process matrix workers; the hosted matrix enables it. Ordinary tests leave
it disabled. The file distinguishes journal reads/appends, lease renewals,
invocation lookup and signal operations; incomplete calls are located using the
stack, not inferred absent from a finish-only timing log. Process-worker rows
retain their existing subprocess diagnostics rather than claiming this observer
covers a separate process. Existing workloads, fault schedule, five-minute
batch bound, per-invocation p99 and all safety/history checks remain unchanged.

## Contract and control

A native three-node contract PASSes under race in 3.34 seconds (4.371 seconds
package time). It retains an actual invocation, terminal journal/outcome and
held lease, finds that owner despite a losing duplicate NAK, reads the correct
raw sequences/values through another node and verifies the lease revision/value
are unchanged afterwards. Four absent resources remain explicit errors, and a
canceled capture records incomplete metadata/context. A compiled diagnostic
code overlay omitting active owners fails in 3.10 seconds on one target instead
of two. This verifies the diagnostic target census; it is not one of the six
runtime invariant mutations. Source hashes, positive/negative logs and the
unpromoted control source are retained. Vet, YAML parsing and 25 script tests
pass. Routine fixture compilation is not counted as a sustained runtime proof.

A full ten-minute local seed-13 journal row is now running with the actual
terminal shortcut and operation timings, prefix /tmp/js-wf-matrix-journal-stall-13.
Its terminal result remains pending. It is not restarted when observation yields.
The prior-source 200-seed campaign continues its independent groups despite
failed groups; those omitted seeds cannot count clean. Comprehensive final-source
simulation, 200 consecutive whole-matrix seeds, five-node 24-hour soak and the
other implementation-plan requirements remain open.

## Completed replay and exercised failure path

The local full seed-13 row now PASSes in 628.86 seconds, with 92 batches, 2,576
invocations and 19 leader kills. Its existing per-seed semantic verifier passes
the actual raw log: final retained counts, workload/progress markers and all
p99 gates agree. Aggregate terminal p99 is 9.018 seconds, worst individual
workload terminal p99 is 15.052 seconds, and worst progress p99 is 7.065 seconds.
Full operation, dispatch, history, fault and latency artifacts are retained
compressed as full-seed13-*. It does not reproduce or erase the original
five-minute stall: seeded fault selection does not fix real server/goroutine
interleaving. Production runtime is ca82e23; the added fixture diagnostics are
covered by source-sha256.json. One passed row does not clear the whole release
matrix or explain the failing prior-source cohort.

A 35-second ordinary journal smoke passes under race in 51.110 seconds and
retains 7,424 operation records. It is explicitly not ten-minute evidence.
A compiled fixture overlay changes the short result to zero, then the expected
result assertion fails in 11.77 seconds. Before failure it captures live stacks,
all ten parent cohort targets with raw metadata, queue state and operations.
The output files and control source are retained as forced-result-*; this is
an exercised diagnostic path, not a real runtime bug or chaos release mutation.
The first attempts of these two shortened runs used a three-minute Go timeout
and were rejected by the fixture's existing duration-plus-recovery precondition;
they never ran and are excluded. The subsequent eight-minute Go bound retains
that precondition without changing the 35-second workload or any acceptance gate.

## Hosted instrumented seed 1 and subsequent bounded snapshot candidate

Run 36855298677 at e5def8bf34a0531c3d2209c9fb22c9cf64c2e794 PASSes its actual
full ten-minute journal seed-1 test. Terminal job/source identity and the existing
per-seed semantic checker pass: 2,520 audited invocations, 19 faults and aggregate
terminal p99 7.327 seconds. Complete raw log, job metadata and the scoped semantic
report are retained as hosted-instrumented-seed1-*. The twenty/200-seed campaign
verifier deliberately does not accept one seed; this report checks the individual
seed and explicitly leaves both release flags false. It does not reproduce or
erase the earlier failure, and predates the subsequent runtime snapshot budget.

A [subsequent snapshot response-budget proof](../worker-snapshot-budget-2026-10-01/)
finds that automatic MaybeSnapshot inherited a workflow's whole lifetime. The
production context gap has a fast Tier 1 reproduction and real R3 withheld-port
contract, and is now bounded to fifteen seconds. This is a candidate stalled
boundary only: the original two failures lack stack/operation data confirming
snapshot work, and no server cause is attributed from this improvement.
