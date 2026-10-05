# Full400k direct callback comparison preparation

Explicit `WF_AUDIT_CAPACITY_DIRECT_CALLBACK_COMPARISON=1`, alongside the plain
copied-store comparison, selects R1 callback /R1 direct callback /R1 callback
recheck. Each mode verifies actual INV/JRN consumer replicas1/1 and cursor start
positions; persisted streams stay R5. Exact400k invocation/journal/terminal and
4.8M entry report must complete within the original20s deadline. Shared scanner
invariant/gap/order/replay/recovery logic and concurrent state reader remain in
use. Comparison uses four-core/GOGC200/2GiB profile with no per-record profiling.

Compile/skip preparation passed; no native capacity result yet. Even a quiet R1
capacity pass does not qualify R1 cursor loss/failover or default adoption. Current
R3 native race integrity/fault controls accepted; legacy qualification running.

Fresh donor copy preflight derives its byte budget from the complete original
closed store inventory, plus320MiB for selected source, executables, logs, delta,
parts, Git object retention and reserve. This uses base-plus-delta preservation,
not the earlier full duplicated600MB archive budget. No cardinality/deadline or
preservation-fidelity reduction. Native launch waits for legacy review and disk
headroom; original failed fixtures are never reopened.
