# Short successful pulls: fixed serial fallback, failed native deadline gate

The installed nats.go v1.54.0 Fetch implementation suppresses timeout/no-message
status errors. A short batch with nil `Error()` therefore cannot prove absence.
The reader now asks the leader for at most one next retained record, visits it,
and resumes bulk delivery from the following sequence if a tail remains. At
most two cursor resumptions are allowed across both explicit transport errors
and successful short pulls. Original captured bounds, deadlines, gap checks,
semantic error handling and final bounded cleanup remain unchanged.

New regressions fail against the previous reader's serial tail behavior and
pass after the change. Race controls pass, including an empty consumer that
repeatedly returns short success, leader-proved absence, semantic read failure,
exact visits, cutoff, cancellation and uncertain-create cleanup.

## Native R5 result — failed, not qualified

A fresh five-container v2.15.0 file-backed cohort has 100,000 completed invocation
records and 1,200,000 journal entries. Normal2GiB/GOMAXPROCS2, explicit routes,
2-minute sync and the original20-second per-attempt audit budget are retained.

| Stage | Result | Elapsed | Cursor starts | Journal point reads |
| --- | --- | --- | --- | --- |
| Baseline | Exact complete report | 18.742181336 s | — | — |
| Admitted explicit timeout after128 | Exact complete report | 18.304644725 s | 1,129 | 0 |
| Admitted short success after128 | Deadline exceeded after1,022,814 visits | 19.997030413 s | 1,130 | 1 |

Each admitted pull drains a real512-message batch but deliberately withholds its
suffix from the visitor. The short-success stage uses one leader read for129 and
bulk resumes at130. Every observed sequence is checked against the next expected
sequence. It avoids the serial-tail defect but does not finish the full report.
Its partial report contains100,000 invocations and zero committed journal totals.
The whole named test fails after305.82 seconds; earlier successful stages are
reported as diagnostics, not promoted into current-source full qualification.
Synthetic loss does not consume a two-second timeout delay or reproduce a
natural server fault. The native-million diagnostic was concurrently delivering.

Complete failed originals include the actual live test executable/hash/build
information, all five inspected container executable copies/build information,
634 unchanged captured Go/module inputs, source overlay, unit/race logs, producer,
installed SDK source used for the contract review, original five physical stores,
and terminal native output. Every archive member and publication part is read
back. Container copies match the fixture's built server bytes. The SDK executable
is captured through `/proc/exe`; server copies are from inspected container paths.
Compiler dependencies are not exhaustively inventoried, and stores have not
been independently reopened.

This is a concrete audit-throughput failure under the unchanged budget. Server
cause is unconfirmed. The native gate, full matrices and24h remain open. Next
work is to measure pull-window/cursor-replication costs before another soak;
no unchanged failing native rerun or deadline relaxation is justified here.
