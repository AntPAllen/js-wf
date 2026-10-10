# Actual canonical graph 100,000-entry boundary

The native continuation limit fixture now supports an opt-in production cap via
`WF_GRAPH_CONTINUATION_LIMIT_BUDGET=100000`. The worker's default cap remains
100,000 and is not overridden in that mode. 49,992 SDK SetState calls, split
across two stages, generate 99,984 real request/completion appends before the
existing checkpoint/suspension/signal/terminal path. No synthetic cursor, bulk
prefill, imported history or lowered cap substitutes for the actual boundary.
Fresh deliveries maintain lease heartbeats throughout long stage execution.

A short R1/archive case at budget20 verifies padding across both checkpoints.
The production run targets R1/archive first with a retained race binary, sparse
frozen checkout, exact source inventories, raw Go events and actual command
exits under a durable user service. The package watchdog is300 minutes; the
context uses the same bound. Temp storage is explicitly retained under its run
root outside `/tmp`. No observation timeout authorizes a restart.

Acceptance requires a closed actual Go/supervisor exit and independent review
of 100,000 ordered records, terminal slot99999, two SDK checkpoints, immutable
prefix stage counts1/1, zero forbidden effects, exact rejected request and
canonical/compatibility terminal agreement. R1/live and both R3 variants,
collection, SIGKILL/VM/power/storage faults, broader admission/retention/import
matrices and all original requirements remain separate. The production run is
unaccepted until those receipts are inspected.
