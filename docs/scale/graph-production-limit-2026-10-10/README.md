# Actual canonical graph 100,000-entry boundary

The native continuation limit fixture now supports an opt-in production cap via
`WF_GRAPH_CONTINUATION_LIMIT_BUDGET=100000`. The worker's default cap remains
100,000 and is not overridden in that mode. 49,992 SDK SetState calls, split
across two stages, generate 99,984 real request/completion appends before the
existing checkpoint/suspension/signal/terminal path. No synthetic cursor, bulk
prefill, imported history or lowered cap substitutes for the actual boundary.
Fresh deliveries maintain lease heartbeats throughout long stage execution.

A short R1/archive case at budget20 verifies padding across both checkpoints.
The production run targets R1/archive first with a retained race binary, sparse
frozen checkout, exact source inventories, raw Go events and actual command
exits under a durable user service. The package watchdog is300 minutes; the
context uses the same bound. Temp storage is explicitly retained under its run
root outside `/tmp`. No observation timeout authorizes a restart.

Acceptance requires a closed actual Go/supervisor exit and independent review
of 100,000 ordered records, terminal slot99999, two SDK checkpoints, immutable
prefix stage counts1/1, zero forbidden effects, exact rejected request and
canonical/compatibility terminal agreement. R1/live and both R3 variants,
collection, SIGKILL/VM/power/storage faults, broader admission/retention/import
matrices and all original requirements remain separate. The production run is
unaccepted until those receipts are inspected.

## Live launch

Frozen source `42058f2bb00657fe1786027f4b08e6a2e6e921ba` is running under
`js-wf-graph-production-limit-20261010.service`. [Launch identity](launch.json)
records the live supervisor and invocation; `state.json` remains uncommitted
operational state while running. The [independent reviewer](review.py) refuses
live/nonzero evidence. After the actual supervisor stops, review with
`python3 docs/scale/graph-production-limit-2026-10-10/review.py`; no acceptance
is inferred from starting the job or from a partial progress log.

[Small development race result](padding20-race.log) and
[actual command/source receipt](padding20-receipt.json) are preserved.

## Actual run failed at the first continuation transition

Original frozen `42058f2` closes native/supervisor exit1 at14:09:08 UTC,
package12658.484s. It completes24996 of49992 real padding operations, then
reports `context deadline exceeded`15.513s after that progress line; heartbeat
and release errors are nil. The300-minute package watchdog did not fire.
Source has a15-second continuation publication bound and first checkpoint
confirmation scans from index0 without a prior pointer. The exact failing
publication subphase and any server-side cause are unconfirmed because the
original fixture recorded neither phase timings nor a final cursor snapshot.
[Independent failure review](failed-first-checkpoint/failure-review.json) and
retained source/binary/raw/terminal evidence preserve the failed actual gate.
No checkpoint or terminal limit acceptance is inferred, and no rerun has
started. The next work is a bounded reproduction of checkpoint publication
cost/deadlines before another multi-hour production-cap campaign.
