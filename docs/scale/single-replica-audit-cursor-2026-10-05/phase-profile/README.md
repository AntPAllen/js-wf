# Instrumented full400k R1 callback audit

Executed `618459747521229721eb6d1e69b8a205e175a995`, SDK SHA256
`5107c916076d2419f8ec4844b055e37e52c1a38728c4e390dbb7cb7914d96a5f`.
Diagnostic PASS36.43s preserves an audit FAIL20.004356665s: original20s deadline
plus callback cleanup join deadline. No capacity/default/fault/legacy/24h pass.
Four-core/GOGC200/2GiB target, fresh verified closed-store copy, persisted R5
streams and observed temporary INV/JRN consumer replicas1/1, start1/1, pointreads0.
The million-timer campaign shares this VM; this is not an isolated benchmark.

| Phase | Start from audit | End from audit | Records | Visitor time |
| --- | --- | --- | --- | --- |
| INV | 0.003045s | 1.160832s | 400000/400000 | 0.280977s |
| JRN | 1.160841s | 20.004352s | 4189934/4800000 | 8.039781s |
| State WatchAll creation | 1.160873s | 1.235001s | Not instrumented | — |
| State watch Stop | 2.983008s | 3.001475s | Not instrumented | — |

Snapshot lifecycle ended early relative to the journal deadline. Stop alone does
not certify the initial-set barrier. This audit failed during scanning and did
not return final state acceptance, so no independent400k state-completion claim
is made from lifecycle timing. Final report INV400k/other0 counts reduction
progress; it does not show stored journals missing.

Journal scan18.843510s; payload295739501 bytes accepted. Per-record timers and
CPU/allocation profiling add cost and may change scheduling/GC; the earlier
unprofiled R1 attempt reached219008 final journal validations, while this
instrumented run never reached final reduction. These runs do not establish a
repeatable performance ratio or attribute a server-side defect.

SDK CPU profile20.01s wall /21.19s sampledCPU includes NATS parser3.89s cumulative,
selectgo3.19s, JSON struct decode2.46s and scanner9.02s; cumulative costs overlap
and must not be summed. Alloc-space sampled2.16GB includes message-processing
1.23GB cumulative and journal decode0.27GB. Sampled allocations differ from the
runtime2.781GB incomplete-attempt total; neither is per-record throughput proof.
External server CPU is outside this SDK profile. Complete original profiles and
exact executed source/executables are in the canonical fixture.

The adapter still hands each record from SDK callback to its `next` reader and
then through a separate batch goroutine/channel to the shared scanner. A next
changed candidate can investigate eliminating one handoff while preserving the
same scanner bounds, gap oracle, order/replay checks, bounded recovery and
cancel/error/join semantics. No unchanged native rerun or production adoption.

Independent review verifies679 selectedGit Go/module inputs, actual SDK bytes/VCS,
five actual server executable/module identities and data mounts, closed observed
processes and1058 unchanged original store files. Complete1708-file logical tree,
823 aliases/3560904031 unchanged bytes,886 new delta members; base plus delta
full readback verified. Delta50033282 bytes/two parts, SHA256
`47361d0e71d198acfa7a5d5d2bdd4f242bbbde2d6449202b6ff54c1308cc1d97`.

Pinned base commit6184597, metadata path
`docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/archive-verification.json`,
SHA256 `970b0adbddef8d8fb76ab8d1b523b26820217c9a1ffd8994544887d26d1c84a0`.
Base Git objects and delta parts are both required. Reconstruction covers regular
file bytes/paths/modes/mtimes; ownership/directory metadata are not included.
Exact pinned verifier retained as `executed-delta-module.py`. Original stores
were never reopened. Source capture does not exhaust external toolchain inputs.
