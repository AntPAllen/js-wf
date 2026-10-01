# Typed fencing evidence from the sustained process fixture

The shared matrix subprocess helper now retains `-fencing.jsonl` with actual PID,
per-process sequence and production `worker.FencingEvent` including worker,
invocation, exact delivery, epoch, reason and error. Each fencing record is synced
before its observer returns. Write/sync failure stops the fixture. A process killed
while writing may leave an incomplete tail; no complete hard-kill attribution is
claimed. Existing dispatch traces remain separate. Graceful exit checks the
fencing count against production metrics and writes `-metrics.json`; killed
processes cannot supply this final cross-check.

The focused native R1 proof runs a real child of the integration test executable
using the production worker. The existing acquisition handoff holds one exact
lease. After four seconds its retained revision is deleted with CAS, then the
handoff is released. This is an explicit test lease-revocation fault, not a server
fault or a recovery latency benchmark. The process reports one execution lease
loss for the selected delivery, retries and returns42. All four recovered journal
entries use an epoch higher than the revoked lease; the retained invariant audit
passes. SIGTERM exits successfully and the final fencing counter equals1.

`focused-race.log` passes8.34s native /9.383s package including acquisition-handoff,
failed-selection cleanup and selected-delivery fencing checks. The retained
process artifacts name actual PID72881. `vet.log` is empty successful vet output.
This improves evidence for subsequent sustained process rows; it does not
retroactively add records to older runs or clear the full R5 process/matrix gate.
The fixture synchronously syncs rare fencing observations; this timing is distinct
from the production CLI's bounded asynchronous event-file writer.
