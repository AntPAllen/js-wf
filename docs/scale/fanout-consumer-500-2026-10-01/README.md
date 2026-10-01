# 500-child consumer-leader fault and terminal wakeup drain observation

The opt-in real R3 fixture commits all 500 children, stops its initial parent
worker at a durable suspension, and checks every physical child WorkQueue
payload/sequence. It kills the parent partition's consumer leader with seven
pending and one ack-pending delivery. The process must actually exit by SIGKILL;
a different consumer leader must be elected while the old process is dead.
After restart all ten workflow stores and the selected consumer replicas must
catch up. The exact parent journal prefix and all 500 child queue identities/
sequences must remain unchanged before successor execution.

All 501 outcomes complete, every child returns 7 and the parent returns 3500
through all three nodes. The raw audit finds 501 invocations/journals/terminals
and 4,504 logical entries. Child latency uses actual invocation commit and
terminal timestamps, without a heal-time adjustment; parent latency starts
at its last completed child. Final observed child p99 is 13.961920265 seconds,
maximum 16.032564558 seconds; parent delay is 2.664501366 seconds.

## Open drain result

The final observed race FAILS in 58.979 seconds at the new fixture's 30-second
physical queue-drain assertion: 360 parent wakeups remain, and the independent
consumer snapshot has 358 pending, one ack-pending and zero redeliveries.
This is a live backlog, not evidence of acknowledged-but-retained records.
The raw-state and terminal latency assertions passed before this diagnostic
failure. The 30-second drain bound is a fixture diagnostic, not a replacement
for the plan's five-minute completion gate or its per-invocation p99 definition.

Parent operation records include 142 full journal reads totaling 14.597 seconds,
with no read errors, plus per-delivery invocation/lease activity. Many occur
after the final parent journal write. Each terminal duplicate currently reads
and decodes the full history before terminal repair, so repeated replay work
is a candidate bottleneck. These client timings do not prove server execution
or account for every interval. Raw operation records, queue/consumer snapshots,
final node metadata and a post-terminal summary are retained. Production
runtime is unchanged; no shortcut or gate relaxation is promoted here.

An earlier final-census race, before requiring queue drain, PASSes in 35.967
seconds (child p99 18.703 seconds, parent 2.660 seconds). It is explicitly not
final acceptance. The first drain-check run also fails (61.826 seconds), with
354 retained wakeups. The unchanged original six-child all-server restart path
passes under race in 19.970 seconds; final vet and workflow YAML checks pass.

## Semantic controls

A compiled fault-injector overlay returns kill success without killing the
process: it fails immediately on the physical process-state assertion in
8.954 seconds. A compiled production enqueue overlay returns success without
publishing child runs: it fails on zero physical children versus 500 in
8.352 seconds. Neither is promoted; neither failure is a build error or timeout.
Their patches/logs are retained. These controls precede operation diagnostics
but exercise the same physical kill/census assertions.

The registered fanout-500-restart workflow adds an independent race job with
operation/server/queue artifacts on failure. Hosted run 36849349075 at d098d0c
reports successful job metadata, but its raw consumer-leader test FAILS in
59.84 seconds with 373 queued parent wakeups. It is not a passing test. Child
raw-start p99 is 14.627 seconds, maximum 16.535 seconds; parent delay is 3.168
seconds. The parent records 129 full history reads totaling 14.883 seconds.
The independent full-restart test genuinely PASSes in 26.78 seconds with 501
invocations/journals/terminals and 4,509 entries. Both complete job logs, job
metadata and all five consumer diagnostic JSON artifacts are retained here.

The consumer step's default shell did not propagate the failed go test through
tee. All four existing Go-test log pipelines now explicitly enable pipefail,
including promise-restart, blob pagination and the sustained matrix. The matrix
already has a separate JSON result guard. Actual edited commands for the three
standalone pipelines were executed with a stub Go command returning zero and
one: each preserves the test exit code and writes its log in both cases. These
six shell controls are recorded in pipeline-exit-controls.json; they verify
exit propagation, not workflow correctness or runtime safety. Prior passing
claims require a raw test PASS, not successful job metadata alone. The initial
summary claiming both 500-child jobs passed is corrected by this raw evidence.

This one backlog cut does not prove the full 500-child fault matrix or the
separate 200-seed sustained matrix and five-node 24-hour soak.

    WF_FANOUT_CONSUMER_500=1 WF_FANOUT_CONSUMER_REPORT=/tmp/fanout-consumer go test -race ./integration -run '^TestFiveHundredChildFanoutSurvivesConsumerLeaderKill$' -count=1 -timeout=8m -v
