# R5 mixed worker SIGKILL row

`TestFiveContainerMixedWorkerKilledEveryFiveSeconds` runs five actual worker
subprocesses against five Docker NATS servers, R5 workflow stores and production
2m write sync. Workers use the production12s lease TTL,13s AckWait and3s heartbeat,
and the existing matrix handlers, including2s short effects. Every five-second
slot seeds worker selection and tries an acquired-delivery handoff for500ms.
A held target remains blocked until SIGKILL is confirmed. If no new acquisition
is available, that outcome is retained and a seeded worker is still killed;
this can happen while the serial cohort awaits redelivery. At least one confirmed
held-delivery kill is required. Every replacement starts the next generation on
the same slot; surviving workers serve all64 partitions.

## Verified smoke

35s race workload PASS59.75s /60.781s package:3 batches,84 completions,927 journal
entries and6 confirmed SIGKILLs. Two kills held exact acquired deliveries; four
record no acquisition within500ms. All histories, retained invariants, per-type
raw terminal/progress p99 and physical WF_RUN/all64-consumer drain pass. Worst
terminal p99 is15.033140398s; worst progress p99 is13.233777373s. No route heal-time
exception applies. Eleven process generations are retained; five graceful exits
have independently matched final counter snapshots. Fencing records are zero;
all51 repair attempts are acknowledged (10start,33signal,8suspended).

The artifact guard validates actual exit signal9, worker/PID/slot/generation
identity, replacement chains, original held-delivery dispatch records, schedule
slots, complete-record counts and final surviving counters. Interrupted tails
are retained and counted separately. Killed generations have no final counter
snapshot; complete hard-kill attribution and full24h matrix acceptance remain
false. All44 Python tests, focused reader race and integration vet pass.

## Excluded attempts

- `excluded-acquisition-timeout`: nativeFAIL28.33s /28.347s package. The first
  held delivery was killed, then an overly strict fresh-acquisition requirement
  timed out during a cohort waiting for redelivery. Selection now distinguishes
  the bounded no-acquisition outcome while preserving random kill cadence.
- `excluded-cadence-guard`: nativeFAIL58.61s /58.644s package after all6 kills.
  An old shared assertion expected one30s fault. It now uses the selected row's
  interval. No acceptance is inferred from this failed run.

Original files are retained, with large files losslessly compressed. Closed
cluster stores remain locally in the original /tmp roots; they are not included
in this repository proof. Binary and source hashes identify the local smoke.
The ten-minute clean-source row remains required.
