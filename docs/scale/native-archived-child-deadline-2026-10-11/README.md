# Archived successful-child native follow-up — 2026-10-11

Both previously failing archived successful-child promise-priority variants now
pass in a focused race run at d771ee3: R1 in 50.21 seconds and R3Domain in
66.82 seconds (worker package 118.102 seconds). The production 15-second append
bound and each fixture's 120-second enclosing context were unchanged. This run
sets GOMAXPROCS=2/GOMEMLIMIT=512MiB and enables the successful-child observer
added in 2035d52. It selects two variants, rather than the previous eight-variant
command run alongside other verification. These environmental/scheduling differences
prevent assigning a cause to the previous two failures.

Each variant verifies two checkpoint frames, one exact child start/terminal
witness pair, a single child execution, cached promise reuse, two parent effects,
35 ordered records, one terminal and result 43. Operation logs show all 35 parent
appends per variant without errors. Legacy WF_JRN remains empty. Raw native
integrity checks run after worker closure; no independent auditor was skipped.

The reviewer binds all 1157 Git Go/module inputs to the exact source, matching
before/after snapshots, the captured and retained race executable, actual exit 0,
and every one of 473 retained native files (29,442,666 bytes). Stores and executable
remain under /home/exedev/js-wf-native-archived-child-deadline-20261011. No local
native proof was deleted or moved. An initial reviewer incorrectly assumed that
checkpoint frames appeared as Checkpoint journal append entries; that refused
reviewer and actual exit 1 are preserved. The corrected reviewer checks the actual
raw checkpoint census separately from journal append operation counts.

This establishes compatibility for these two variants at this source. The earlier
SDK native failures remain historical failures with unconfirmed causes. No automatic
uncertain retry was introduced, no deadline was relaxed, and no claim is made for
a complete current native matrix or the failed 100,000-entry race campaign. The
retained failed race store remains available for recovery investigation. Full
current Tier-1 and original release gates remain separate. Public continuation
admission stays disabled.
