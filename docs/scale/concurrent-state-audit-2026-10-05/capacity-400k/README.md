# Full 400k R5 audit capacity failure

Executed source `a124b698d9793d92f59154a9380416d7f125790b`, actual SDK595173,
normal profile, GOMAXPROCS2/GOMEMLIMIT2GiB, five current2.15.0 file-backed replicas,
explicit route seeds /2m sync. All400,000 invocations, 4.8M contiguous journal
entries and400,000 matching terminal states populated with every publish ACK
checked. Source stream metadata asserts exact message counts and R5 file storage.
This is a fresh quiet synthetic population, not a live workload/fault soak.

Native FAIL276.09s. Allthree audits hit their original20s deadlines:

| Mode | Elapsed | Allocated bytes | GC cycles |
| --- | --- | --- | --- |
| Sequential | 20.000098239s | 4,551,692,856 | 73 |
| Concurrent state | 20.000093818s | 4,761,258,400 | 39 |
| Sequential recheck | 20.000118637s | 4,365,056,616 | 56 |

Partial reports have400,000 invocations andzero journals/entries/terminals.
These are incomplete audit counters, not a claim that the persisted journals or
terminal states are absent. The full report is populated after scan/reduction;
more phase evidence is needed to locate the limiting work. Allocation/GC costs
are measurements, not proof of the failure's cause. No NATS defect attribution.

Actual SDK SHA256 `6c43fd3c18676a00a256104cd7f7f70ddee10c010a16f45061dd25f8a1883dca` is recorded in independent-review.json along with the
observed executable build metadata and five actual NATS executable observations.
Selected Git Go/module source matches executed revision before/after; captures
are not an exhaustive external compiler/toolchain input proof. Observed processes
are closed; originals were not reopened. Complete closed source, executable,
servers, logs, verdicts and raw stores are in the split read-back-verified archive.

The earlier fb0cc9e provisioning failure remains preserved separately. Preparation
was changed to bounded3s provision calls within4m; audit limits stayed20s.
No unchanged native rerun. Concurrent reader remains experimental, no400k capacity
pass, default adoption, actual24h or fullmatrix qualification is claimed.

Complete archive: 1,752 members /24 parts /605,567,553 bytes, SHA256
`970b0adbddef8d8fb76ab8d1b523b26820217c9a1ffd8994544887d26d1c84a0`. Every archived member and part read back.
