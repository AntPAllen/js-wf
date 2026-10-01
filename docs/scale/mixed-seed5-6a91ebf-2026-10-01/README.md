# Seed 5 run-queue drain failure

Source 6a91ebf0f791469954b129dcc2c59cfdb56e0831,
[hosted run 36821732316](https://github.com/AntPAllen/js-wf/actions/runs/36821732316).
Mixed seeds 1–4 passed; seed 5 failed its unchanged 30-second run-queue drain
requirement. All 28 invocation outcomes completed and terminal p99 was 5.950
seconds. Failure happened after the audit, with WF_RUN Msgs=1, first sequence
101, last sequence 138 and 22 consumers. All three final snapshots show
WF_P_27 num_ack_pending=1, num_pending=0; the other consumers have neither.

Faults: 25 ms delayed node 1, node 0/1 route partition, node 2 pause at 24 ms,
node 0 kill at 58 ms. Final stream leader is node 1. Monitoring snapshots are
instantaneous observations, not per-message history. No raw sequence-101 payload
or acknowledgment timeline was captured by this fixture, so neither invocation
identity nor server cause is established. The outstanding acknowledgment is
consistent with uncompleted delivery; these artifacts do not prove an already
acknowledged message was retained.

Preserved seed-5 fault schedule, histories, physical disk trace, all monitoring
cuts, server logs and failed-job transcript. The independent lease-pressure and
lease-disk jobs passed. This is a queue-drain failure rather than another
terminal-latency miss. No latency, renewal or acknowledgment requirement was
relaxed. A clean nearby campaign does not clear this failure or the full gate.

## Subsequent operation-log correlation

The original review above did not correlate the retained operation log. That
log identifies sequence 101 as mixedsignal/mixed-06-0 with eight ErrHeld
acquisitions at five-second intervals, while other wakeups for the same
invocation release successful leases. The identity is therefore established
by runtime operations; raw message headers and an ACK commitment are still
unproven. See [terminal duplicate proof](../terminal-held-wakeup-2026-10-01/)
for selected operations and controlled seeded/real reproduction. This new
analysis does not label the original failure a server ACK-retention bug.
