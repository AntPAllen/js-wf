# Plain full400k compact metadata capacity comparison

Executedce885e0, actualSDK622717, normalGOMAXPROCS2/GOMEMLIMIT2GiB, five restored
R5 file servers/current2.15, explicit routes/2m sync. Fresh disposable copies
bound to the canonical original400k/4.8M archive and original hashes. Existing
stream/state metadata asserts400k/4.8M/400k; no workers or provisioning.

NativeFAIL77.08s. Plain comparison omits CPU/visitor profiling:

| Reader | Elapsed | Allocated bytes | GC cycles | Verdict |
| --- | --- | --- | --- | --- |
| SDK concurrent | 20.000731154s | 4,211,864,608 | 46 | Deadline exceeded |
| Compact concurrent | 20.000075684s | 3,521,392,712 | 28 | Deadline exceeded |
| SDK recheck | 20.000083065s | 4,701,667,072 | 53 | Deadline exceeded |

All attempts retain original20s limits and read complete retained state rather
than cached prior results. Partial counters400k/zero reduction entries are not
a claim that persisted journals/terminal state are absent. Lower compact allocation
in these incomplete attempts does not establish per-record or full-audit speedup.
Zero-allocation native metadata/parsing controls remain useful, but this candidate
is insufficient to qualify the current full400k capacity profile. No NATS defect
attribution. Candidate/default/memory/deadline rules were not changed mid-run.

Independent selected Git source/actualSDK and five NATS executable/data-mount/PID
closure review; all1058 original store files unchanged. Complete closed copies,
source, executable, rawstores and all three verdicts archived/read-back verified.
External compiler/toolchain input capture is not exhaustive. No unchanged native
rerun, default adoption,400k pass or24h/fullmatrix qualification claimed.

Complete archive: 1707 members/24 parts/605615891 bytes,
SHA256 `42a31ccd95d7056753558a81b433788b81bd878eef88952015efb9d109b047ee`; every member and part read back.
