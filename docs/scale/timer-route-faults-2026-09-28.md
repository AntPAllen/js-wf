# Timer route-fault audit

The opt-in `WF_TIMER_SLEEP_CHAOS=1` test extends the 10,000-sleep workload in
[`timer-sleep-2026-09-28.md`](timer-sleep-2026-09-28.md). It starts with every
timer scheduled and at least half still active. A three-node route mesh then
isolates nodes 2, 1, and 2 for eight seconds each, verifying route counts on
every cut and heal. Two seconds into a cut, a pre-created worker on the
majority node begins pulling the isolated owners' partitions. The durable
consumers use a 10-second `AckWait`, two-second progress heartbeat, and 32
concurrent messages per partition. A timer reconciler scans from the majority.

This test is a **failing release gate**. In a full 10,000-invocation run, all
10,000 timers were scheduled and 9,387 remained active before the first cut.
The completed handler count stalled at 9,735. It was still unchanged more
than five minutes after the latest possible `fire_at`, so the diagnostic run
was stopped. That run preceded the final bounded-read changes. A later
100-invocation run with 1–10-second sleeps completed every workflow but had
48.1 seconds p99 handler lateness, above the plan's 30-second chaos target.
The exact 10,000-case gate has not yet passed with the latest changes.

The fault runs exposed several recovery gaps that now have bounded retries:
the timer reconciler no longer exits on a transient stream response or a
cursor CAS conflict; `Client.Await` retries bounded reads; worker timer
publication, server-time lookup, journal read, and invocation/signal lookups
use bounded attempts; empty pull loops refresh consumers when messages remain
unhandled. These changes preserved the no-fault 10,000-sleep result at
826 ms p99 in a subsequent full run. The current code still needs the route
fault gate to pass consistently, followed by the broader chaos matrix.

To reproduce the smaller diagnostic run:

```sh
WF_TIMER_SLEEP_CHAOS=1 WF_TIMER_SLEEP_COUNT=100 \
  WF_TIMER_SLEEP_MAX_SECONDS=10 WF_TIMER_SLEEP_TIMEOUT_SECONDS=70 \
  go test ./integration -run '^TestTenThousandRandomSleepsDuringRouteFaults$' \
  -count=1 -timeout=2m -v
```
