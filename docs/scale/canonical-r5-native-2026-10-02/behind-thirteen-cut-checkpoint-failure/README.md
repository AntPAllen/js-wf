# Thirteen-cut behind attempt: workload paused by checkpoint

Run36967016515 atc477f00 is terminal/failed in486.744s. It reaches13 completed
fault records with13 corresponding clock-cut files, then the14th admission
expires without a new provable pending timer. Retained receipts contain1400
subjects, all with observed Completed tails, the last at05:10:52.410840591.
The next fault is scheduled05:10:55.936284117, during the synchronous batch50
raw-state audit. Cancellation at05:11:06 interrupts that audit before it can
report journals or terminals. The full gate remains failed: there is no final
latency/history/physical-drain/state acceptance. Original uploads and terminal
API/log evidence are retained without rewriting the failed fault.

The source runs a whole retained-state checkpoint before starting the next
batch. With every observed timer already complete, that pause supplies no
positive first-duration wait for the10s clock admission. The fixture now uses
the existing completed-cohort checker independently, captures each WF_INV
cutoff before admitting later work, joins all audits, and retains mandatory final
whole-state checking. Future CI also requires every expected checkpoint artifact.
This changes workload scheduling, not duration/source/cut or latency gates.
