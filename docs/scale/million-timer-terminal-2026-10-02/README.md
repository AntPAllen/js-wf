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

## Retained-message identity and drain diagnostics

A fresh copy of the original three stores was observed for60s on isolated ports.
The metadata API reports141 native `wf.schedule.volume.<id>.0` sources under
the elected leader. Eight source messages were fetched without deleting or
publishing anything. Every decoded payload, generation header and step matches
its durable receipt slot; each delivery sequence is later than its source
sequence. Thus the sampled sources remain despite their validated deliveries.
All original store-file hashes remain unchanged after this second inspection.
`message-review` preserves all45 monitoring snapshots, stream metadata, eight
raw API messages, receipt cross-checks, logs, process identities and exact script.
The source-retirement failure's underlying persistence/replication cause remains
unconfirmed; copied-store observations cannot retroactively certify the failed run.

The production campaign now records each final inspection in fsynced
`drain-audits.jsonl` and exposes `last_drain_audit` in its report. Stream and
consumer counts are nullable: null means not observed. Metadata errors retain
the failed stage/consumer index and the count of consumers already checked.
The original shared3s inspection deadline, campaign deadline, delivery lateness
limits, and physical zero-message/all64-consumer drain requirement are unchanged.
An explicitly incomplete final audit makes offline verification fail, even if
legacy final count fields are zero. Old reports without the added field retain
their existing verifier contract; the original failed report remains rejected.

The full timer-volume package passes under race, including all new retained-source,
partial metadata, context cancellation, nullable JSON and false-pass verification
cases plus the actual receipt-process-kill parent test. Its child-only helper
intentionally skips at top level. The initial review incorrectly rejected that
helper skip; the inventory correction is retained, with no test rerun.
The compiled control ignores retained messages in both inspection and verdict;
the actual named retained_sources test fails with `incorrect drain verdict`.
`drain-diagnostics` preserves original positive/negative Go JSON events, stderr,
exact overlay/source hashes and review correction. Vet passes. Every published
archive member is compared by SHA256 on readback. No24-hour rerun is launched.
