# Full400k quiet R5/R1/R5 callback comparison

Executed source `f2361d9df5dc8f93651e1ea1c3d5a82d0c4eb25a`, normal
four-core/GOGC200/2GiB memory target. Native test FAIL in77.24s. Each audit retains
its original20s deadline and exact400k INV /400k journals /4.8M entries /400k terminal
acceptance gate. This quiet workflow population shares the VM with the live
million-timer campaign; it is not an isolated-machine performance measurement.

| Mode | Elapsed | Reduced journals | Error |
| --- | --- | --- | --- |
| Callback R5 | 20.000526s | 0 | Deadline plus callback cleanup join deadline |
| Callback R1 | 20.000091s | 219008 | Deadline |
| Callback R5 recheck | 20.001934s | 0 | Deadline |

Every mode reads all400k invocation records and verifies actual INV/JRN cursor
replicas (5/5,1/1,5/5), start sequences1/1 and zero point reads. R1 reaches the
final journal validation/reduction phase: reduced report219008 journals,
2628096 entries and219008 terminal. This implies the preceding complete journal
scan and state snapshot returned successfully under the executed control flow;
exact phase timings were not instrumented. R5 zero reduced journals does not
mean stored journals are absent. All modes fail the full capacity gate.

Incomplete allocation totals2.528/3.201/3.004GB and GC20/10/14 are not throughput
or per-record speedup evidence. No server-side defect is attributed. No R1
adoption, failover/legacy/live/default/24h qualification is claimed. Next changed
diagnostic should measure R1 scan/state/final-validation phases to locate the
remaining deadline cost; do not repeat this unchanged comparison.

Independent review verifies678 selected Git Go/module inputs, actual SDK SHA and
VCS identity, five actual v2.15.0 server executable hashes/module identities,
restored data mounts and observed process closure. All1058 original store files
remain unchanged. An observer race with Docker container teardown printed a
transient missing-container error and was handled; allfive required server
observations were retained. External compiler/toolchain inputs are not exhaustive.

## Canonical preservation

Base-plus-delta schema `js-wf-lossless-fixture-delta-v1`:1698 logical files,
824 aliases covering3560904081 bytes,875 delta archive members. The complete
virtual inventory, all base members/parts/archive and delta members/parts were
read back and verified. Delta49952663 bytes in two25MiB-or-smaller parts,
SHA256 `6616fd55a31b378d55527460fffe264ba7029b7597f28a201929783fff4a2880`.

The pinned base is at executed source f2361d9, path
`docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/archive-verification.json`,
SHA256 `970b0adbddef8d8fb76ab8d1b523b26820217c9a1ffd8994544887d26d1c84a0`.
Both its Git archive objects and this delta are required for reconstruction.
Changed store files and new source/executable/log/review records are preserved
in full. File bytes/paths/modes/mtimes are covered; ownership and directory metadata
are outside the contract. The exact pinned verifier is retained in the delta as
`executed-delta-module.py`; use the preparation instructions to reconstruct into
a fresh destination. Original stores were never reopened.
