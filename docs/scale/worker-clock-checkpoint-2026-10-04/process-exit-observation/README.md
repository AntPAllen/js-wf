# Report unexpected clock-worker exit before stale-sample failure

The downloaded seed-6 raw upload from full run 37164231641/job 111324439684
shows all five actual clock-worker subprocesses fail on `worker clock proof:
context deadline exceeded` after 4.67–5.21 seconds. The parent reports a stale
sample at its first periodic check about 30 seconds after the initial proof.
The child failure precedes and explains that secondary error. The raw upload
cannot distinguish publish from broker-message-read timeout; server cause
remains unconfirmed. Server logs contain consumer quorum/election churn during
startup, which is an observation, not proof of the probe timeout's cause.

The worker-clock fixture now observes each actual child Wait result through
its existing fleet-error path, retaining worker identity, exit error, child log
path and the last 4 KiB of the child's error text. This reports and records the
primary failure before cancellation. Cleanup cancels observers before signaling
children; consumed exit results leave closed channels so already reaped children
remain safe to clean up. Only the clock row has this observer: its processes
have no planned exits. Clock probe errors now identify publish versus broker
message read and retain wrapped errors and sequence where available.

The two-second probe deadline, 500 ms publication cadence, four-second sample
freshness, worker/process success checks and all workload targets are unchanged.
This is an observation correction, not a broker recovery fix or new matrix pass.
No replacement full campaign is dispatched on this evidence.

Three focused tests pass normal/race in **0.006/1.027 seconds**: an actual OS
child exits with status 7 and its clock-proof error reaches the parent observer;
cleanup remains safe after that observation; an unplanned successful exit fails;
cancellation preserves the cleanup Wait result. A compiled Go overlay disabling
the observation fails all three tests in **0.004 seconds**. It demonstrates the
error-preservation oracle, not a reproduction of the old full campaign. All four
edited input hashes agree before the race/control executions and afterward;
the normal run preceded that ledger capture. Executables are not
retained. Exact source, overlay, controls, logs and hashes are in `proof.tar.gz`;
all outer members were independently read back and verified before publication.
