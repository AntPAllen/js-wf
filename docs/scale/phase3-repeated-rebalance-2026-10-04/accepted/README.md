# Repeated worker faults with partition rebalance independently accepted

Executed source `e9ad9aba93cabfbe765e17e57f136e0b1c9c7779`. Named retained race test
`TestPhaseThreeCountersRebalanceWithRepeatedFaults` passes in 293.03 seconds.
All 200 workflows on six worker processes complete 50 counter steps in
192.806391 seconds, within the unchanged 300-second target.

The trial executes 96 worker faults (81 SIGKILL, 11 actual process pauses,
4 client partitions), with active-lease observations for every fault category.
All long holds last at least 45 seconds. There are 87 worker process instances,
148 successful assignment moves (129 with active-lease observations), four
recorded no-stream-response move errors, and a confirmed 45.055304-second
server minority route hold. Assignment moves use stable logical worker IDs.
Two-second worker fault and five-second assignment schedules remain unchanged.

Independent raw review verifies 200 journals /20,400 entries, contiguous indices,
monotonic epochs, one writer per epoch, paired increment steps and terminal
result 50. All 200 journals independently replay through the exact tested handler
and SDK; all 29 actual replay dependency inputs match executed source.
The source-bound named test reports final integrity 200 invocations /200 journals
/200 terminal records. Physical stores have not been independently reopened;
no separate queue-drain proof is claimed.

Actual SDK SHA-256: `062fe5ece4a83e92a79d3ccb5c22993f637dc4cf72c1071fc524c084f4799f4f`.
All 3,850 captured selected Go/Cgo/test/module inputs verify, including 563 local
inputs against exact Git bytes, unchanged before/after execution. This inventory
is a superset, not exhaustive assembly/embed/generated/hermetic provenance.
The actual replay executable and 477 original store files /20,921,316 bytes are
retained. NATS servers are embedded in the actual SDK; separate server OS
executables are not claimed.

Canonical proof: 5,076 members /53,514,879 compressed bytes /
3 parts. All member contents, unchanged inputs, parts and concatenated
archive readbacks verify. Concatenate parts in manifest order and verify the
canonical SHA-256 before extracting into a fresh directory.

This qualifies the focused combined repeated-rebalance case at its executed
source. It does not establish the sole cause of the earlier failed trial, qualify
all Phase3 cases, full current-source matrices or the actual 24-hour soak.
