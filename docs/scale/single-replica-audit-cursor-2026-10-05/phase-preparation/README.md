# R1 full400k phase measurement preparation

One changed diagnostic: `WF_AUDIT_CAPACITY_SINGLE_REPLICA_PROFILE=1` uses the
shared callback reader with observed actual R1 temporary INV/JRN consumers,
keeping persisted streams R5, full400k/4.8M population and original20s deadline.
It records each scan start/end, record/payload counts and visitor time, CPU and
allocation profiles, and state-watch creation/stop timestamps. The watch wrapper
embeds the SDK watcher directly; no additional relay queue or updates goroutine.
Stop completion alone does not certify the state initial-set barrier. Scan and
watch timestamps identify residual work after successful reads; they do not split
sorting, key enumeration and final journal validation into independent timings.

Profiling and per-record timers add measurement cost. Diagnostic PASS can contain
a failed audit, separately recorded. No native/default/fault/capacity acceptance
is inferred. Compile/skip preparation and race initial-set contract tests passed;
closed partial initial sets still fail and valid sets retain their values.
Native execution and independent canonical base-plus-delta review pending.
