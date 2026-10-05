# Full400k fixedR1 direct callback capacity failure

Executed4ea20c176280557a43a68668a63c823333c41dfd, native FAIL77.37s.
Normal four-core/GOGC200/2GiB profile; exact400k INV /400k journals /4.8M entries /
400k terminal acceptance within original20s. All persisted streams R5; actual
INV/JRN cursor replicas1/1, starts1/1 and pointreads0 verified in every mode.
The VM also runs the live million-timer campaign; no isolated-machine benchmark.

| Mode | Elapsed | Reduced journals/entries/terminal | Error |
| --- | --- | --- | --- |
| R1 callback | 20.000136s | 6469/77616/6468 | terminal-state lookup context deadline |
| R1 direct callback | 20.000774s | 0/0/0 | Deadline plus callback cleanup join deadline |
| R1 callback recheck | 20.000054s | 0/0/0 | Deadline plus callback cleanup join deadline |

Every invocation scan completes400k. The first mode reaches final validation;
its6469th journal returns `terminal state missing: context deadline exceeded`.
That error records deadline exhaustion, not evidence of absent stored state.
Zeros in later reduced reports do not show stored journals missing. No candidate
capacity pass or repeatable direct-delivery benefit is established. Incomplete
allocation3.137/3.029/2.773GB and GC19/15/11 are not per-record throughput proof.
R1 metadata overlap/failover is still unqualified; no default or24h adoption.
No server-side defect attributed. Do not rerun this unchanged comparison.

Independent review verifies680 selected Git Go/module inputs, actual SDK bytes/
VCS/module, five actual v2.15.0 server executable/module identities and mounts,
closed observed processes and1058 unchanged original store files. Full1685-file
logical tree,818 aliases/3560903781 unchanged bytes,868 delta members verified.
Delta49997931 bytes/two parts, SHA256
`a6d4d431222603112844f6397a9b9b00740fd0461edac9f231a703d1d4bcbc6d`.

Pinned base4ea20c1, metadata path
`docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/archive-verification.json`,
SHA256 `970b0adbddef8d8fb76ab8d1b523b26820217c9a1ffd8994544887d26d1c84a0`.
Complete base archive/member/part and virtual delta inventory readback checked.
Both pinned base Git objects and delta parts required for reconstruction; file
bytes/paths/modes/mtimes covered, ownership/directory metadata excluded. Exact
pinned verifier retained as executed-delta-module.py. Original stores never
reopened; source capture excludes exhaustive external toolchain inputs.

Next changed diagnostic can measure CPU/GC headroom with an explicit enlarged-VM
memory/GC profile; retain full cardinalities and20s deadline and distinguish that
configuration from the failed2GiB/GOGC200 gate. No quiet-capacity configuration
alone qualifies real fault recovery or public defaults.
