# Copied active-population store inspection — 2026-10-11

The original failed native race store (2346 files, 10,652,843 bytes) was never
opened by these probes. Two independent copies matched its complete byte census
before opening. `review.py` verifies frozen helper inputs, actual executables and
terminal receipts, both closed copy censuses, and the unchanged original.
The probes start copied NATS servers and read source, journal, lease and consumer
metadata. They do not start workers, pull messages, or recover Starts. Graph
reads may update reader metadata in the copies.

The first helper's initial build failed on an unused import; its receipt and
source are retained. The corrected build passed but the probe exited 1 while
observing old-00003 under a grouped 8-second deadline. That observation does not
identify a particular API subphase. The second independent helper separates
8-second phases and reads live invocations first. It exited 0 in 17.680 seconds.
Two first-round metadata deadlines are preserved; later bounded read-only
readiness checks and every invocation/consumer observation succeeded.

All 20 Starts are bound, match their source sequence, and contain the expected
kind/index input. Ten old journals contain Started and Completed. Eight live
journals contain only Started. live-00001 (partition 51) and live-00005
(partition 63) have empty journals. All ten live dispatch messages remain in
WF_RUN at sequences 11–20. Consumers 51 and 63 report one pending message,
zero delivered consumer sequence, and zero acknowledgment pending.

Both undelivered partitions are assigned to worker index 3 by the fixture's
six-worker static partition scheme. None of the successful old invocations
exercises that assignment. These are committed restored snapshots, not live
handler counts or a reconstruction of the exact stall timeline. Restored leader
and activity timestamps do not prove the original cluster's behavior.

A separate SDK control reproduces a fatal expired FetchContext classification:
[bounded dispatch fix](../dispatch-expired-fetch-option-2026-10-11/README.md).
That demonstrates a runtime defect consistent with an assignment stopping;
it does not establish the original native failure's cause. The full active,
purge/reuse, signal and raw-audit gates remain open.
