# Independently verified sustained membership recovery

[Hosted run36958387092](https://github.com/AntPAllen/js-wf/actions/runs/36958387092),
sourcec7f38e2e170f116dbbbe59a7880199f8b1fc59eb, passes683.52s test time and the
actual ten-minute workload. Current independent row/event/fencing reviewers pass:
1,736 terminal invocations,19,133 journal entries and19 journal-leader kills.
Worst per-type terminal/progress p99 are15.629884296s /13.040560128s. Histories,
retained invariants, immutable outcomes, physical drain and counter gates pass.

The run exercises28 stopped/rejoined member sessions across all five configured
workers:5/5/5/5/8 rejoins for workers0/1/2/3/4. Each records a higher acknowledged
epoch. There are643 held/retry observations, no unfinished final recovery, and
783 acknowledged assignment writes with complete64-partition CAS chains.
Stop reasons are24 no-stream-response lease losses,3 context deadlines and1
lease loss caused by a deadline. All145 repairs are acknowledged.

All four retained fences have matching completed-after-fencing timelines. One
lies outside confirmed server-fault windows. It is a context-canceled delivery
on partition42: the assignment changes from worker2 to worker4 at
03:20:39.071962335Z, and worker2 records the fence at03:20:39.072277168Z. This is
consistent with partition-loop cancellation after a move; retained timing does
not prove callback causality or a server defect. The raw outside-window count
and exact assignment chain/delivery correlation are retained. No fence is hidden.

All downloaded artifacts, full job log, run API, independent reviews and the
explicit correlation are gzip-retained with verified original hashes/sizes in
original-sha256.json. To reproduce, decompress and use the three current Tier3
reviewers on the retained artifact root with row auto_journal and duration10m.

This clears one sustained fixed-fleet membership/journal seed with actual session
recovery. It does not clear200 seeds, arbitrary membership joins/leaves, combined
coordinator/reply/clock faults, final-source release or the full five-node24h matrix.
