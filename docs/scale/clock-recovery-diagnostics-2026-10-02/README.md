# Fault recovery diagnostics

The preceding admitted ahead seed5 smoke passed its confirmed source-exit timing
but failed the later five-current-replica check for KV_WF_LEASE. Retained logs
show quorum-stall warnings on healthy nodes as well as the shifted node, after
routes reconnect. These observations do not establish the cause of the stall.
The error's `%+v` stream rendering hid nested cluster details behind an address.

Readiness failures now serialize full stream/consumer info with nested leader and
replica state, and retain request errors. Nil metadata is handled explicitly.
All Tier3 mixed controller failures capture independent public monitoring for
JetStream/Raft, routes and connections on all five nodes before fixture cleanup.
Each snapshot includes controller observation brackets and either original JSON
or an explicit error. Node reads run concurrently with two-second request budgets
inside a shared six-second budget, independent of the failed workload context.
Recovery and latency deadlines are unchanged; diagnostics cannot turn failure
into acceptance. A snapshot describes state at capture time only.

`checks.log` records focused race checks for nested replica details and partial
monitoring failure, including a blocked node, invalid JSON and HTTP errors.
The capture retains all15 records without using failed bytes as successful data.

The instrumented seed5 smoke at2875042 failed in91.7s at physical drain. Its
fault healed and all six per-type latency checks ran, then three actual schedule
subjects remained. Their Wf-Timer-Deadline values are11:15:47–49Z while stored
Nats-Schedule times are11:16:47–49Z. The report and retained reads are preserved
in `ahead-smoke-drain-review.json`. This identifies pending native hints after
completed workflows; it does not establish a server cause. No fault-controller
failure snapshots are expected on this run because its controller healed.
The existing drain-before/after independent monitoring captures are retained.
