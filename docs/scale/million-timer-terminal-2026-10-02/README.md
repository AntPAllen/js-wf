# Million native timers: delivered, final drain unproven

The original 24-hour service at92586ea ended with exit1 and
`campaign deadline: context deadline exceeded`. It published and received all
1,000,000 distinct schedules across R3/file stores and two actual all-server
SIGKILL/restart cycles. Receipt-derived p99 lateness is0.251500673s and maximum
15.976507332s, within the configured2s/30s delivery limits. Three redeliveries,
zero acknowledgment errors and zero fetch errors are reported.

Offline recovery with the original campaign binary, against copied report and
ledger files, validates every slot checksum, distinct stream sequence, and both
controller/server timestamps against its timer deadline. It recovers all1M
receipts. The original binary's release verifier rejects the failed report with
`incomplete or incompatible campaign report` and exit1. Recovery never marks
this run passed. Final message/pending fields in the failed report are defaults;
there is no successful physical drain audit behind their zero values.

The final drain loop uses one3s deadline for stream metadata and sequential
metadata requests across64 consumers, and silently retries errors/nonempty
state. The old logs do not distinguish retained messages from metadata failure.
Neither a client-budget problem nor a server scheduling defect is established.
Investigate copies of retained stores and add explicit drain diagnostics before
another long campaign. No original campaign or stores were restarted.

`originals.tar.gz` preserves the failed report, complete durable receipt ledger,
original campaign log, terminal service state, offline recovery observations and
report, and rejected verification output/exit. All9 members were SHA256-compared
on readback and listed in `manifest.json`. Original physical stores and binary
remain local at `/tmp/js-wf-timer-volume-million-service-20261001` and
`/tmp/js-wf-timer-volume-92586ea`. They are not part of this archive.

This is delivery evidence only. Neither the million-timer release gate nor the
separate full-runtime24-hour soak has passed.

## Isolated restored-store observation

All three store copies were reopened on fresh localhost ports, using the pinned
original server binary and server names. Five monitoring snapshots per node were
recorded over20s, then all inspection processes were joined. Every original
store-file SHA256 was checked before/after and remains unchanged.

The last snapshot reports WF_RUN message counts768/141/0 across nodes0/1/2,
with node0 the reported stream leader and all64 consumer pending/ack-pending
counts zero on each node. Last sequence is2000000 on every replica. This is
evidence of retained physical messages and divergent local store state in the
restored copies, not proof of metadata-deadline exhaustion or its original cause.
A zero-message follower cannot certify leader drain. Inspect retained message
subjects/headers and replica convergence before attributing this to the server
or changing any drain gate. The inspection restarted only copies, not originals.

`store-copy/originals.tar.gz` preserves every top-level diagnostic file, including
all15 full monitoring snapshots, logs, process/port identities, exact inspection
script, original-file hash inventory, unchanged-store proof, and summary. Every
member was compared by SHA256 on readback. Modified copied stores remain at
`/tmp/js-wf-million-store-copy-20261002` and are excluded from that archive.
