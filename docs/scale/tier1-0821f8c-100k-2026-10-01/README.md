# Expanded hosted Tier 1 campaign at 0821f8c

[Run 36807819775](https://github.com/AntPAllen/js-wf/actions/runs/36807819775)
finished successfully October 1 at 04:43:56 UTC without restart. Exact source:
0821f8c48c843c518fa3ccbe73f2f1d7703ff14b; 100,000 seeds per workload.

The retained full log reports 8,300,606 generated schedules, 170,756,712 choices,
1,997,383,080 transport events and virtual time up to 7,200,000 ms. Go test time
was 6704.961 seconds (1h51m44.961s). The resolved-promise workload contains
expected corruption-rejection controls; those must not be described as successful
workflow executions. This confirms the suite at this older revision, before
later canceled-timer behavior, active-continuation cancellation and terminal
replay identity validation. It is not a full final-source release gate or
real-cluster evidence. The newer 7e414b2 expanded campaign remains live.
