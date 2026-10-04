# Worker-clock seed 15 retained-audit deadline

Full Tier3 replacement run **37164231641**, job **111324439617**, exact source
`79915ca41a5c5a23b9997eea5f3f66d82530ee30`, fails the requested worker-clock
seeds 14–26 shard. Seed 14 passes its full-duration workload/package and producer
review; seed 15 fails, and seeds 16–26 do not execute. Neither the shard nor its
full parent qualifies. Other running jobs are preserved while this failure is
investigated; the healthy Tier2 campaign is unchanged.

## Confirmed observations

Seed 15's batch-90 checkpoint, cutoff 2,520, starts at
`00:54:48.428247569Z` and fails at `00:55:48.428799897Z`. Its partial report is
2,520 invocations, 1,302 journals, 12,833 entries and 1,301 terminal values. The
error is a terminal-state **read returning `context deadline exceeded`**, wrapped
as `terminal state missing`. This does not establish a missing key, corruption
or server cause. The checkpoint cancels the shared workload context; an active
batch-100 fanout Await then reports `context canceled` and masks the checkpoint
cause in the terminal test log. The fixture still records the real cause in
`checkpoint-audits.json`. The test/package fail at 618.54/618.592 seconds.

The preceding batch-80 checkpoint passes in about 17.22 seconds. Audit capacity
and transient transport are hypotheses requiring stronger evidence. The existing
helper performs at most three 20-second whole-cohort attempts inside 60 seconds;
the failed artifact records only the final partial report, so it cannot prove
each earlier attempt's timing or progress. No bound or acceptance target is
relaxed on this evidence. This differs from the earlier noncurrent clock-probe
snapshot failure.

All **759** pre/post source files for both executed seeds match exact Git input.
Raw artifact **11288829478** is 5,218,672 bytes. Original executable/store artifact
**11289735245** is 221,236,365 bytes in its GitHub ZIP; its canonical nested
`rolling-originals.tar.gz` is 221,374,721 bytes with SHA256
`3845cc4c2f18fb5a349c0e383490e2abb29651927ee51904ecb60cfd5a6b3f26`.
All **7,892** canonical members independently hash-verify against the producer
manifest. These originals are not reopened or repaired.

## Diagnostic change

The R5 fixture now records every checkpoint attempt's start/end/deadline, partial
report and error. Failed attempts and the primary checkpoint failure are logged
before shared cancellation can obscure them. Existing three-attempt/20-second
attempt/60-second total bounds and all workload latency/integrity gates remain
unchanged. The focused existing audit cadence, invariant/exhaustion and
cancellation tests pass (`js-wf/integration`, 0.004 s); the updated fixture
compiles. This adds observation, not a recovery fix or new qualification.

A focused full ten-minute seed-15 diagnostic is dispatched as run **37166976657**
at exact `c2e8c0122c15ef2d31ffa11c24ccd53c203a233f`. The API confirms that head
and live planner **111331818100**. [Exact dispatch metadata](diagnostic-dispatch/)
retains the command, source and API handles. This is one diagnostic execution;
it is not a full-matrix replacement or qualification. Existing jobs continue.

`proof.tar.gz` retains complete raw JSON/logs, terminal metadata, full original
producer manifest, independently verified original-archive digest, source
comparison, failure analysis, exact diagnostic diff and the focused test log.
Every outer member was read back and SHA-verified before publication.

The large original binary/store archive is excluded from the repository archive
because local disk is constrained. It remains unchanged at
`/dev/shm/js-wf-tier3-clock-failure-37164231641-111324439617/original-stores/`
and in the [original GitHub artifact](https://github.com/AntPAllen/js-wf/actions/runs/37164231641/artifacts/11289735245),
whose recorded expiry is 2027-01-02. The RAM copy does not survive VM reboot;
restore from the GitHub artifact and verify the recorded digest/manifest.
