# Full126 race qualification — terminal review

Passed the original race1000 run at frozen `cf99bc0d3acc0fcec3ce639ee1804c1711db4d75`: 126,000 workload bodies, 433 pinned regressions and 184 top-level passes; two documented trace-only skips. Wall time was 1,976.87 seconds under the original 60-minute SDK deadline. All 2,444 selected inputs match Git and the before/after inventories; the actual SDK and original supervisor identities match the committed launch. Fifteen altered proof controls were rejected. The unused absolute deadline timer was stopped after terminal review.

[Independent review](independent-review.json) and [complete archive manifest](archive-verification.json) preserve the evidence. This qualifies the frozen simulation source only. Current production code, normal100k, native matrices, online GC, million-timer physical drain and actual24h remain separate gates.
