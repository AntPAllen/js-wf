# R5 mixed worker pause-past-lease smoke

`TestFiveContainerMixedWorkerPausedFortyFiveSeconds` starts five actual worker
subprocesses against five Docker NATS nodes, R5 workflow stores and production2m
write sync. A seeded active worker is SIGSTOPped at+5s and once per minute, with
its stopped OS state confirmed. Current KV snapshots prove it holds retained
leases and preserve key, worker, epoch, revision and server timestamp. It stays
stopped for45s, past the production12s lease TTL, while peers serve its partitions.
SIGCONT resumes the same PID; fencing after resume is required. Raw enabling-event
latency gates remain unchanged. Ten minutes require ten actual pauses.

## Verified smoke

35s workload race PASS114.38s /115.408s package, including startup and the full
45.081337440s pause:5 batches,140 completed invocations,1,550 entries. All six
mixed workload cells, histories, retained invariants and physical WF_RUN/all64
consumer drain pass. Worst raw terminal p99 is18.249581437s and progress p99 is
6.979746641s. All five original process generations exit gracefully; final
counter snapshots match the actual fencing records. One heartbeat lease-loss
record after resume belongs to the retained paused lease at epoch403, PID85158.
Its invocation completed while the original owner was stopped. The original
delivery trace, terminal observation and exact retained lease snapshot are
preserved in `fencing-causal-review.json`. The old epoch is rejected by KV revision
CAS after resume. All57 repair publications are acknowledged (32start,19signal,
6suspended) with checked source/decision explanations.

All46 Python tests pass, including controls for short pause, wrong process/epoch,
missing held lease/fencing, interrupted records and mismatched final counters.
Integration vet passes. The existing worker-kill smoke still passes its updated
artifact guard. Binary/source hashes identify this local smoke. Large raw files
are losslessly compressed; original closed server stores remain in the /tmp root.

This is focused R5 smoke with one full pause. It does not establish the ten-minute
row,200-seed matrix,current-source comprehensive simulation or full24h Tier3 gate.
The historical30s-TTL worker-kill smoke is not repeated.
