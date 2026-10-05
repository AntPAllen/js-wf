# Full400k R1 direct process-fault capacity qualification prepared

`TestConcurrentStateR5CopiedCapacityProfile` now accepts
`WF_AUDIT_CAPACITY_R1_PROCESS_FAULT=owner-down` or `owner-restart`, in addition
to the existing absolute verified-copy root and restored donor identity.
Each mode requires its own fresh full donor copies; failed originals are never
reopened. No native execution is claimed: VM free disk remains below the roughly
3.58GiB fresh copy plus SDK/archive/review headroom required.

The fixture verifies exact400k INV/STATE and4.8M JRN R5 file populations and
full replica catch-up. It first requires a current-source healthy baseline,
then faults at journal visit128 after verifying actual pending named R1 memory/
AckNone cursor and actual owner. SIGKILL source exit/pre-kill logs are recorded;
the restart mode uses the same node/store. Both baseline and fault require exact
400k INV/journals/terminal,4.8M contiguous once-only journal visits and zero audit
consumers within each original20s deadline including cleanup. Readiness is
separate from the audit budget. Invalid mode is rejected before cluster startup.

Results persist before verdict assertion: actual consumer identities/replicas/
starts and full API snapshots/errors, per-name deletion replies/times, target/kill/restart proof, elapsed audit and total time, every consumer
cleanup observation, allocations/GC and actual runtime memory limit/CPU count.
Expected removed-container logs are skipped only for the recorded killed node
with confirmed exit and retained pre-kill log. Per-fault full report/deadline
failure remains a failed test. Compile/opt-in skip passes; private/default reader
selection, original cardinality/deadline and replay/recovery rules unchanged.

Future execution must preserve actual SDK and every original/restarted server
binary/module/PID/mount, selected source, unchanged canonical donor and complete
closed base-plus-delta reconstruction proof. The existing quiet18.35s pass and
1500/6000 fault proof do not qualify this full fault gate.
