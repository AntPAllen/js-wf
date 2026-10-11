# Worker uncertain append recovery — 2026-10-11

The new graph_worker_append_deadline family runs the production worker and graph
journal over seeded in-memory graph, lease, invocation, outcome and dispatch
transports. Request, completion and terminal cuts run in JSON and protobuf-v1,
with four fault modes: grant read deadline, noncommitted CAS deadline, committed
CAS with unavailable readback, and committed CAS resolved by exact readback.
The model injects request deadline responses while the enclosing context remains
live. It does not sleep to simulate a request timeout.

## Recovery contract

The first worker stops after its actual ACK or NAK. Each cut checks authoritative
journal population, absent uncertain outcome projections, no durable Failed entry,
zero uncertain retries, and effect/handler counts. Publication CAS counts exclude
reader metadata changes. A new worker reacquires the released lease and reads the
same store. Resolved cases instead receive an explicit duplicate wakeup.

All cases finish with exactly Started, StepRequested, StepCompleted and Completed,
matching sequence/index and exact terminal projection/result. Committed completions
replay the recorded effect once. An effect whose completion did not commit can
execute twice; this preserves the plan's idempotent effect requirement and does
not assert exactly-once external execution. A committed terminal is recovered
without invoking the handler again. Legacy journal writes remain zero. Physical
reference checks, dispatch drain and retirement/sweep prove no modeled objects
remain, including abandoned upload attempts.

This uses graph journals with ordinary invocation bindings, not canonical Start
publication, checkpoint/archive child flows, power loss or a real NATS cluster.
The 30-second modeled lease and three-second modeled AckWait are fixture choices;
no worker-kill recovery target or production timeout is changed.

## Evidence and scope

The focused normal and race commands require 1,000 completed schedules each,
exact generated replay, all 24 pinned cases and default/explicit failure-trace
persistence. run.py records actual exits and captures both actual test executables.
review.py checks 2105 unchanged Go/module/CI/fixture inputs and the exact new CI
job guard. Inventory grows from 162 families/922 pins to 163 families/946 pins.
Both final commands pass (normal 9.564 seconds; race 134.221 seconds). Full
current-source qualification is separate; frozen full162 excludes this family.

An exact one-condition Go overlay permits uncertain append retries. It fails at
seed 1 with an observed retry renewal. The original failure replays, minimizes in
five reproductions and the minimized failure replays. Production worker behavior
is unchanged. Logs, overlays, saved failures and actual receipts are retained.

Development failures are retained: SIM_SEEDS=100 was rejected by the existing
1000-seed minimum; the first ID mapped to partition 60 while the fixture ran
partition 0; a relative trace destination was resolved against the sim package;
and an initial counter incorrectly included reader metadata CAS changes. None
is presented as a runtime defect. The large wrong-partition trace was compressed,
uploaded to S3, freshly read and hash-verified before removing local bytes.
wrong-partition-s3.json binds that object and its uncompressed digest. The initial
AWS CLI attempt could not launch; signed curl performed the verified transfer.

These modeled outcomes support conservative fresh-delivery recovery. They do
not confirm a cause for the archived native child failures or the 40,959-entry
race failure. Retained native recovery investigation, archived-child compatibility,
full qualification and original fault/scale/soak/adoption gates remain open.
Public continuation admission remains disabled.
