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
