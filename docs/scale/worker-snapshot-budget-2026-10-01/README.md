# Bounded automatic worker snapshot attempts

The worker's automatic MaybeSnapshot call previously inherited the entire
delivery context. Snapshot metadata/object requests can therefore outlive the
normal journal-read and append budgets while the heartbeat keeps ownership alive.
A five-minute caller context is sufficient to expose the missing request budget
in Tier 1. This is a concrete runtime liveness gap under a missing-reply contract,
not a claim that snapshot work caused the real journal seeds' five-minute stalls.
The original failures contain no operation/stack evidence identifying that call.

## Runtime change

Ordinary worker automatic snapshot work now has a fifteen-second context, matching
the initial journal-read budget. A timeout follows the existing NAK/release path;
publication/purge uncertainty is repaired on redelivery. Parent context/handler
cancellation, mandatory append renewal, fencing, thirteen-second AckWait and
three-second heartbeat remain unchanged. A terminal hint is enabled only after
successful automatic snapshot work, preserving repair on retry. Continuation
handlers own checkpoint publication and do not use this automatic operation.
Optional operation records now include journal_snapshot and its full duration.
No I5 target, route heal definition or queue-drain requirement is relaxed.

## Deterministic reproduction

The new worker_snapshot_response_budget workload invokes the same production
worker, journal, lease, outcome and signal decisions. Its snapshot port stalls
one metadata reply and inspects the actual context supplied by the worker. It
rejects a lifetime over fifteen seconds, models the worst-case bounded wait by
advancing virtual time 15,000ms, then returns DeadlineExceeded. This checks the
context boundary without wall-clock sleeping or inventing a server mechanism.
The lease fixture uses a thirty-second TTL; native heartbeat/12-second expiry
maintenance is verified separately, not claimed from this virtual slice.

Completed, Failed and Suspended modes verify pending retry, release, unchanged
terminal journal/outcome, effect replay, signal resume, physical queue drain and
the shared retained-state checker. First ten schedules replay exactly, two
processes generate identical seed-42 traces, and three pinned modes remain in
the 135-trace corpus. 100,000 seeds PASS in 21.291 seconds with 6,796,634 events
and maximum virtual time sixteen seconds. The unmodified baseline fails in
0.004 seconds on inheriting a five-minute lifetime. A compiled production
control passes the parent context instead of the bounded child: the model
fails in 0.004 seconds and the native fixture fails in 3.41 seconds on that
specific context property, not a build failure or five-minute timeout.

## Real three-node contract

A race-instrumented fixture uses real R3 invocation/journal/state/leases, with
one withheld snapshot-port metadata response. It does not claim to drop a NATS
wire reply or recreate the original server fault. A workflow records one effect
and suspends awaiting a signal. During the missing response, the actual worker
heartbeat performs renewals and a competing client remains ErrHeld after the
original twelve-second TTL. The port honors the supplied context at fifteen
seconds; the worker releases/retries, consumes the enabling signal and completes
with one effect and two handler calls. The original five-entry suspended prefix
is unchanged; result/duplicate start, empty physical queue, no retained lease
and the shared raw integrity checker all pass. Final log reports 19.38-second
test time, 16.187-second first-start-to-drain and 15.090-second raw signal-to-
terminal recovery, three heartbeat updates, one snapshot timeout and one
invocation/journal with eight entries and one terminal. No heal-time adjustment
is used for this quorum-preserving port-response fault.

The first native attempt wrongly required three updates on thirteen-second
observation and saw two because the existing recent-renewal optimization can
skip alternate ticker boundaries. It is retained and excluded from runtime
failure claims. The final check requires two real updates plus actual ErrHeld
past the original TTL; the retained fencing check and 30-second recovery gate
are unchanged. Observation time is relative to the received request deadline,
so diagnostic readback time cannot move the check past timeout.

## Regression and scope

Full sim/worker/journal suites PASS in 97.559/30.023/0.005 seconds; all previous
pins stay byte-identical. The 135-pin corpus and new workload PASS under race in
7.466 seconds; vet and all 25 script tests pass. The real 500-child consumer kill
still PASSes under race in 30.32 seconds with all 501 outcomes, retained audit,
physical run/consumer drain, two full reads and two successful snapshot calls.
Raw operation/server reports and all positive/control logs are retained with
source hashes. Native retained-state assertions are not substituted by the
context-property controls.

The original five-minute journal stalls remain unconfirmed. A separate hosted
instrumented journal seed 1 at e5def8b PASSes its ten-minute fixture and semantic
checker, but predates this snapshot budget and does not reproduce the failure.
Whole final-source 100,000-seed simulation, 200 clean seeds across all variants,
five-node 24-hour soak, remaining transport/continuation/scale/operational gates
remain independent and open. Hosted confirmation of this exact runtime revision
is pending; the goal is not complete.
