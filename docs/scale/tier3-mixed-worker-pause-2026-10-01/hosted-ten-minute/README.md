# Hosted ten-minute R5 worker pause proof

[Run36925290442](https://github.com/AntPAllen/js-wf/actions/runs/36925290442)
is terminal SUCCESS atba824e2c02796aaaa871987ea50b8d03a299418c.
Named race PASS717.30s; package PASS718.325s. Ninety batches complete2,520
invocations with27,905 journal entries and ten confirmed45s pauses. Original
lease snapshots, same PID/generation, full pause/resume times and typed resumed
fencing reverify. Each pause has fencing for a retained owned key/epoch;14
records match these snapshots. All five graceful counters match all15 records.
Timeline review classifies four already-terminal duplicate fetches, two cases
completed during the original delivery and nine while the owner was stopped.
These categories are kept distinct; not every held lease represented an
unfinished invocation at the cut.

All mixed histories, retained invariants, raw terminal/progress p99 and physical
WF_RUN/all64-consumer drain pass. Worst terminal p99 is15.026555325s; progress
p99 is0.417671403s. All168 repair publications are acknowledged (12start,
125signal,31suspended) with checked source/decision explanations. The fixture
retains production12s lease TTL,13s AckWait,3s heartbeat and R5/2m write sync.

This closes one ten-minute source/row, not the200-seed or24h mixed matrix.
Original hosted server stores are absent from the download. Large raw artifacts
are losslessly compressed; local ack observations alone are not broker-commit
proof. No precise server-side cause is inferred by the timeline reviewer.
