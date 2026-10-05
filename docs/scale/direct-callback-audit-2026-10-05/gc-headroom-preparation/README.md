# Enlarged-VM full400k GC-headroom comparison

Explicit four-core/GOGC500/4GiB diagnostic configuration replaces only the earlier
comparison's GOGC200/2GiB SDK profile. Same source algorithms, normal build,
R1 callback/direct callback/callback-recheck modes, persisted R5 streams and
actual observed R1 INV/JRN cursor configuration/starts. Complete400k invocation,
400k journals,4.8M entries,400k terminal report and original20s deadline unchanged.
The earlier2GiB/GOGC200 failure remains failed. This changed runtime profile does
not isolate GC percentage from memory-limit effects or qualify that earlier gate.

Each attempt now records HeapAlloc before/after, heap/runtime system bytes,
NextGC, cumulative GC CPU fraction (not a per-attempt ratio), GC pause delta,
cycle/allocation deltas and actual `/gc/gomemlimit:bytes` runtime metric. Reviewer
requires actual4GiB memory limit. Producer samples SDK /proc VmRSS/VmHWM/VmSize
every0.5s; these are observations, not an exhaustive final peak or server memory
measurement. Sampling overhead and the shared live million-timer VM are explicit.

VM has15GiB total and13GiB available before preparation; no swap. Resource
headroom is sufficient for this finite diagnostic. Disk preflight retains full
closed-store clone bytes plus320MiB preservation/source/executable reserve.
Canonical base-plus-delta preservation retains all changed/new file bytes and
metadata. Compile/skip preparation passes; native result pending. Original stores
remain closed. The last978-file/3580352059-byte disposable copy was reclaimed
only after complete pushed canonical proof/current bytes, closed SDK/five server
PIDs and all visible task descriptor checks.

A quiet R1 capacity pass alone does not authorize R1 failover/replay/public-default
adoption or actual24h qualification. Replicated R3 integrity/fault/legacy proofs
remain distinct. No cardinality/deadline reduction or unchanged native rerun.
