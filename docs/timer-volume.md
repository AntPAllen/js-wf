# Native scheduled-message volume campaign

The plan's scheduled-message risk proof is one million native messages due over
24 hours, with two full cluster restarts and every message firing within tolerance.
`cmd/wf-timer-volume` runs that workload on three separate NATS processes using
three-replica file storage. It provisions the production stores, publishes through
`worker.ScheduleTimerWithPort`, and drains the 64 partition consumers directly.
It tests the scheduled-message primitive, not a million workflow handlers.

## Full campaign

Build from a clean committed checkout. The root must be a new directory.

```sh
go build -o /tmp/wf-timer-volume ./cmd/wf-timer-volume
/tmp/wf-timer-volume -root /tmp/wf-timer-volume-million
```

Defaults: 1,000,000 schedules, 24-hour first-to-last deadline span, 15-minute load
runway, 64 publishers, raw delivery p99 at most 2 seconds and maximum at most
30 seconds. The campaign fails if it cannot acknowledge all publishes before
any timer is due. Deadlines are evenly spaced, including both endpoints.

At one-third and two-thirds of the deadline span it joins the readers, verifies
the stream's replicas are current, SIGKILLs all three servers before restarting
any, verifies the same file stores have a leader and current replicas, and
attaches the retained consumers. It records each PID and confirmed SIGKILL exit.
Missing consumers fail; recovery does not create replacements. Metadata/setup
requests have short attempt deadlines independent of the 24-hour campaign.

Every delivery checks its identity, target partition, generation, step, server
publish timestamp and receipt time against the expected deadline. A second
stream sequence for one identity fails. Redelivery of the same sequence is
counted separately and allowed; confirmed acknowledgments and a final zero
stream/consumer backlog are required. Raw lateness includes restart outages.
Any missing timer fails the overall deadline, five minutes after the last due
time. The runner does not advance clocks or relax the full campaign limits.

The root retains server logs and stores, atomic `report.json` progress updates,
and a final `observations.bin`. Each scheduled index has one 16-byte record:
little-endian uint64 stream sequence followed by int64 first-receipt Unix
nanoseconds. A zero sequence represents a missing delivery. The report includes
the binary observation SHA-256, Go/NATS versions, source revision and dirty flag,
workload configuration, restart times, errors and raw lateness statistics.
Interrupted progress retains `running`; it does not count as a pass. Stores are
left in place for diagnosis, and the campaign cannot resume an interrupted run.

After completion, verify every observation independently:

```sh
/tmp/wf-timer-volume -verify -root /tmp/wf-timer-volume-million
```

The verifier checks the hash, record count, unique nonzero sequences, each
receipt deadline, calculated p99/maximum, queue drain, both confirmed three-PID
kills and restart progress. Release verification additionally requires exactly
one million schedules over 24 hours, the default-or-stricter lateness limits,
and a clean committed source revision. Server timestamp/header validation runs
at receipt; the binary artifact records the sequence and receipt time.

## Short fixture check

```sh
/tmp/wf-timer-volume -count 1000 -horizon 45s -lead 30s -p99-limit 30s \
  -root /tmp/wf-timer-volume-smoke
/tmp/wf-timer-volume -verify -allow-smoke -root /tmp/wf-timer-volume-smoke
```

The short check deliberately permits 30-second raw p99 because outages occupy a
large part of its 45-second span. It is excluded from the million-message and
24-hour gates. The Sep 30 check retained [the report and binary observations](scale/timer-volume-smoke-2026-09-30/report.json):
1,000/1,000 delivered, zero redeliveries and ack errors, 28 transient fetch
errors, two confirmed full kills, zero final backlog, raw p99 13.12 seconds and
maximum 13.25 seconds. Offline smoke verification passed; release verification
correctly rejected its smaller scope. Earlier runner attempts exposed an
unbounded provisioning request and a post-restart consumer update waiting until
the short campaign's deadline; bounded setup and retained-consumer attachment
address these fixture issues, without asserting a server-side cause.
