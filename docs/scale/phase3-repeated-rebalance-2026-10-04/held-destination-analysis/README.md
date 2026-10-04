# Held assignment destinations in the failed repeated-rebalance trial

Executed source: `eb5fbd6a984ed0c9ba32864ee0cc9a50676ccb1c`.
Local retained evidence: `/tmp/js-wf-phase3-rebalance-ready-20261004`.

The trial missed the unchanged five-minute target. Comparing successful assignment
move timestamps and destination slots with confirmed pause/client-partition hold
intervals identifies 130 of 231 moves onto already-held workers. The synchronous
controller's destination filter was lost during its asynchronous refactor.

The controller now reads an atomic availability mask maintained by the fault
controller. Existing 45-second holds, the server minority route cut and replacement
initialization exclude new destinations. The route cut publishes minority
ineligibility before applying the cut. A fault beginning after an assignment's
selection can still overlap that assignment; this is expected concurrent behavior.
If a replacement briefly leaves only the existing owner eligible, that partition
waits for the next controller tick rather than making a no-op assignment.
The six workers, four busy partitions, two-second fault cadence, five-second move
cadence, 45-second holds and five-minute completion target remain unchanged.

The 128 report completions are ordered Await result reads, not the global terminal
count. Logs contain 175 distinct ACK observations and 186 distinct fetch
observations. ACK observations do not independently prove immutable results or
final integrity. This analysis identifies a fixture omission, not the sole cause
of the missed deadline or a confirmed runtime/NATS defect. Corrected native
execution and final integrity/replay checks remain required.

The JSON is a derived local diagnostic; it is not a complete archival proof.
Actual failed executable, captured source, events and stores remain local.
