# Verified copied 400k audit phase and CPU profile

Executed `64ece3e52952c08ce75af8b81ff6daf284c5cf5f`, actual SDK610767,
normal2GiB/GOMAXPROCS2 profile, five restored R5 file-backed2.15 servers,
explicit route seeds /2m sync. Each original store file and its disposable copy
matches the manifest extracted from the canonical committed600MB archive;
all original store files remain unchanged after execution. Existing streams
assert400,000 invocations /4.8M entries /400,000 terminal states before profiling.
No workers or provisioning; original stores never reopened.

Diagnostic native test PASS35.37s means instrumentation and cleanup completed.
The measured full audit **fails its original20s deadline** at20.000048073s.
Partial counters are400k invocations /zero completed reduction counters, not
an absence of retained journals or terminal states.

| Phase | Delivered records | Data bytes | Scan time | Visitor time (within scan) |
| --- | --- | --- | --- | --- |
| WF_INV | 400,000 | 2,000,000 | 2.860823s | 0.702322s |
| WF_JRN | 2,610,401 of4,800,000 | 184,250,795 | 17.136284s | 6.343280s |

The deadline expires during journal scan, before complete reduction. Exact
measured allocations3,756,611,480 bytes /62GCcycles. CPU samples total18.17s;
8.82s labelledWF_JRN /1.13s labelledWF_INV. Labels do not cover every asynchronous
client goroutine, so these labelled CPU totals are not complete phase CPU costs.
Timing instrumentation adds overhead; this is a diagnostic, not a clean capacity
qualification or controlled attribution of the uninstrumented failure.

Allocation samples place approximately973.6MiB in NATS processMsg,744.1MiB in
metadata token parsing,323.5MiB in time.newTimer,287.5MiB in message Metadata,
208.0MiB in the scanner and181.5MiB in journal decoding. These are sampled flat
allocations, not retained heap sizes. Sampled allocation totals include startup
work; the exact allocation delta above covers the measured audit. Client transport,
metadata and iterator timer costs deserve measurement before journal-decoder
changes. No server defect attribution or claim that GC alone caused the failure.

Independent source/executable/actualserver/mount/closure/original-byte review and
complete closed store/source/profile archive accompany this record. Selected Git
Go/module capture is not an exhaustive external compiler/toolchain input proof.
No unchanged original native rerun, candidate adoption,400k capacity pass or24h
qualification. CPU/alloc profiles and full pprof output remain in the archive;
summary output and structured phase results are available alongside this README.

Complete archive: 1715 members /24 parts /605643584 bytes.
SHA256 `2aae3d0069407c20cac66c891f1256586dea157bbc6364ad5b1b40626761add4`. Allmembers and parts readbackverified.
Original store files independently unchanged: 1058.
