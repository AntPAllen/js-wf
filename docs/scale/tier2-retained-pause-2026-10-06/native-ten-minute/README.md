# Original Tier-2 worker pause ten-minute native row accepted

At clean d893bb0 the actual normal seed1 native test passed in637.04s:83batches,
2,324terminal invocations,25,675 journal entries,ten45-second SIGSTOP/SIGCONT pauses
holding actual leases. Each pause has post-resume fencing records for the exact
held invocation key and epoch. All six workload cells, checkpoint/final retained
integrity, linearizable histories, raw enabling latency and physical drain pass.

Actual SDK/threeworker binaries/three NATS binaries/closure,694selected Go/module
inputs and3,286captured external inputs were independently verified. Full6,323
member98,384,365byte proof/fourparts passed readback, including original stores.
First reviewer rejected the equivalent Go duration string10m0s; failed script/log
are preserved. Corrected reviewer compares600seconds and nanosecond timestamps.
The native test was not rerun and original stores were not reopened.

Independent copied-store integrity/history/drain is next. This is one sustained
seed, not full13x200/currentfullmatrix/24h qualification. Long campaigns overlapped;
no isolated performance claim. Process observations are periodic, not exhaustive.
