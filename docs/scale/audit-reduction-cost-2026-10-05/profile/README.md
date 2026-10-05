# Full400k/4.8M CPU-only reduction profile

At6adad6e the instrumented diagnostic passes7.69s; allthree phases process
4.8M JSON entries. The two reduction phases each finish400k journals,4.8M entries
and400k terminals. Invocation count is zero because no invocation scan is modeled.

| Phase | Elapsed | Allocation delta | GC cycles |
| --- | --- | --- | --- |
| Decode only | 2.855939965s | 387,234,152B | 10 |
| Predecoded maps/protocol/sorted finish | 1.047043257s | 186,237,256B | 2 |
| Decode plus reduction/finish | 3.737463105s | 573,374,872B | 5 |

CPU labels sample2.80/1.06/3.66s respectively. Allocation profile samples all
phases/setup together: UnmarshalEntry734.56MiB, reduction205.09MiB and sorted
parallel finish110.02MiB dominate. These cumulative samples differ from per-phase
allocation deltas. Selectedsource/actualSDK/module/closure independently reviewed;
complete696-member /onepart /19,138,071byte archive read back.

Explicit four-core/GOGC200/2GiB profile. Reused raw bytes, contiguous subject order,
constant terminal lookup and decoder identity validation/profile instrumentation
are included. No transport, INV/KV, retained-state reads or faults are exercised.
This is a CPU diagnostic, not a native fullaudit/capacity pass. It does not
attribute the native failure to one transport component.

Inference: substantial remaining cost exists outside this isolated reducer;
measure bounded delivery buffering/pipeline costs before another unchanged
fullpopulation attempt. Defaults/full400k/24h qualification remain open.
