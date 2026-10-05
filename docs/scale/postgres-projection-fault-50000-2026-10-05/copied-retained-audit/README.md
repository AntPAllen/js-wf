# Complete copied retained-state audit — qualified

Original production/native source: `9fbfa16`; helper Git source: `11997cd`.
A separately observed SDK opened **only fresh byte-identical copies** of the
closed three-node stores from the qualified full projection case. All 2,352
original closed files remain unchanged before/after this review.

Complete streaming audit: **5.490 seconds**, whole audit/drain body **5.531 seconds**,
under the original 20-second single-attempt budget. It mechanically checks the
complete retained streams and state, without a cohort cutoff: 50,000 invocations,
50,000 journals, 100,000 entries and 50,000 terminals. WF_RUN physically zero on
all three peers; every one of 64 partition consumers has zero pending/ackpending.

Actual review SDK, exact helper Git bytes, 1,664 selected source/dependency inputs,
original/copy media hashes and closed copies independently verified. All 4,035
archive members/two parts read back and verified. Scripts, executable and copied
stores are in the archive; original native media are in the sibling streaming
proof archive. Concatenate numbered parts in order for the gzip tar.

No concurrent writers or injected faults during this copied audit. Library NATS
servers are embedded in the captured SDK, without separate process hashes/logs.
Selected inputs exclude exhaustive compiler/assembly/embed provenance. This proof
qualifies full copied retained integrity/drain; the original projection recovery
verdict is supported separately by its native run. No historical-cause/full-matrix/
24h qualification, and no original store reopen. Two live campaigns shared the VM;
no resource-pressure cause is claimed.
