# Full400k direct R1 capacity accepted under explicit GC-headroom profile

Executed08a90c83818cd25fdff1c2771d2421788026e8ae, native PASS74.00s.
Mandatory direct-reader fullcapacity gate passes18.350495815s within original20s:
400000 INV,400000 journals,4800000 entries,400000 terminal values. The two callback
control audits fail20s deadlines plus cleanup join deadlines; overall test PASS
must not be interpreted as every individual audit passing.

Explicit normal four-core/GOGC500/4GiB SDK profile, persisted R5 file streams,
actual temporary INV/JRN consumer replicas1/1, starts1/1, pointreads0 all modes.
Runtime `/gc/gomemlimit:bytes` confirms4294967296 bytes in every attempt. Shared
million-timer VM and fixed test order mean this is a configuration-specific quiet
capacity result, not an isolated benchmark or repeatable speed ratio. The earlier
GOGC200/2GiB direct failure remains failed. Two runtime settings changed together;
no isolated GC-versus-memory-limit causal claim.

| Mode | Elapsed | Full report | GC cycles | GC pause delta |
| --- | --- | --- | --- | --- |
| R1 callback | 20.000244s | No | 8 | 15.385ms |
| R1 direct callback | 18.350496s | Yes | 3 | 1.688ms |
| R1 callback recheck | 20.000085s | No | 5 | See comparison.json |

Passing direct attempt allocates3297835776 bytes, HeapAlloc before498838744 /
after1112477496, HeapSys1416265728, runtimeSys1471085400, NextGC1404060634 bytes.
Cumulative GC CPU fraction0.00437691836 covers process lifetime, not a per-attempt
ratio. Incomplete control allocations are not comparable per-record throughput.
SDK /proc samples every0.5s observe maxRSS/highwater1413776KiB (~1.35GiB), max
virtual3299392KiB; this is not an exhaustive final lifecycle peak or external-server
memory measurement. Observer work and prior modes can influence scheduling/heap.

Actual SDK SHA256ff84fe15b1ff0df440b1301ad9c43781853535ff887929e2245428792604f088.
Independent review verifies680 selected Git Go/module inputs, actual SDK/VCS/module,
five actual v2.15.0 server executable/module identities and mounts, observed closed
SDK/server PIDs and1058 unchanged original store files. Source/toolchain capture
is not exhaustive outside the selected inputs. No server-side defect attributed.

Complete1686-file logical tree,826 aliases/3560904181 unchanged bytes,861 delta
members verified by full base/archive/part/member and virtual inventory readback.
Delta50020066 bytes/two parts, SHA256
`4ddc309b707ad1602ce11f51519b3fae7969af6c05b7c0cf158aa5f358748e82`.
Pinned base08a90c8, metadata
`docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/archive-verification.json`,
SHA256 `970b0adbddef8d8fb76ab8d1b523b26820217c9a1ffd8994544887d26d1c84a0`.
Both pinned base Git objects and delta parts required. File bytes/paths/modes/mtimes
are covered; ownership/directory metadata are not. Exact pinned verifier retained
in delta as executed-delta-module.py. Original stores were never reopened.

No public-default adoption, earlier2GiB capacity, R1 cursor failover/replay,
fullfault matrix or actual24h qualification. Prior replicatedR3 correctness,
consumerleader/cancel and legacy proofs apply at recorded sources; they do not
qualify changedR1 cursor failure behavior. Next work must qualify R1 cursor/server
loss and cancellation with the shared gap/order/recovery invariants before any
live/default change. No unchanged large quiet rerun is needed for this result.
