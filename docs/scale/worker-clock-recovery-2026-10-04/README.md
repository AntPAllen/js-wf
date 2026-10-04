# Worker-clock capture qualification and full-matrix replacement

## Accepted corrected ten-minute case

[Run 37161589577](https://github.com/AntPAllen/js-wf/actions/runs/37161589577),
job 111317377477, succeeds at exact `070dd954a63eb8d36dc40040805baaf5cc00f0aa`.
Its complete ten-minute worker-clock seed 1 passes independent raw review:

- 105 batches, 2,940 invocations, 32,539 journal entries and 19 periodic proofs.
- All 21 initial/periodic/final clock proofs have five current file replicas,
  with 105 actual broker messages and five distinct worker identities.
- Worst terminal/progress type p99: 5.145831729/0.404114782 seconds.
- Named/package elapsed: 948.74/949.791 seconds, including clock-worker builds.
- All 3,963 original archive members restore and SHA-verify. Original physical
  stores and all three actual clock executables are retained, not broker-reopened.
- Complete 755-file before/after source ledgers match exact Git. Every raw
  row report, checkpoint, event explanation and fencing review regenerates.
- A separate retained checker executes the unchanged production Start, Signal
  and Await history models: all three return `Ok` on 3,780 raw operations.
  Checker SHA256 is
  `4085e42409d1b98dc8aab99fac435ee4386df1cc3694038684ec2fd2d7931b3b`.

The [accepted proof](accepted/manifest.json) retains 14 outer members, including
the complete original store/executable archive, metadata/logs, source/input
ledgers, raw reviewer result, independent model executable/source/output and
qualification summary. Expanded raw inputs are not duplicated in the outer
archive: restore the nested original archive using its recorded manifest.
The outer archive is split into five numbered parts. All inner members, outer
members, parts and reassembled bytes have independent SHA readback. This
qualifies only this corrected full-duration case, not 200 seeds or full release.

All Go/module, R5 fixture/workflow and scenario-review inputs equal reference
`79915ca41a5c5a23b9997eea5f3f66d82530ee30`; later differences are supplemental
Python reviewer/tests and documentation. No production runtime change is made
by the capture correction, and no old failed snapshot is converted to a pass.

## Additional rejected snapshots and cancelled parent

Old full campaign 37157123048 at `98a9254` has three failed worker-clock shards.
The previously retained seed 1 rejection is joined by:

| Job/range | Last executed seed | Retained rejection |
| --- | ---: | --- |
| 111303440224, 40–52 | 43 | Periodic node 1 snapshot: `current=false`, `lag=1` |
| 111303440156, 53–65 | 53 | Periodic node 4 snapshot: `current=false`, `lag=1` |

Both actual named workloads/packages pass before the unchanged reviewer rejects
their raw snapshot. Re-running the reviewer reproduces both exact rejections
without executing the workloads. Seeds 40–42 had successful producer checks,
but their failed requested shard is not qualified and omitted seeds do not count.
These observations establish the same missing catch-up boundary, not a new
runtime defect or a confirmed NATS cause.

The full census before cancellation had no successful row job, three failed
shards, four active worker-clock shards and 249 queued jobs, plus the successful
planner. Continuing this known defective dispatch could not clear its required
whole-campaign gate. Cancellation is now independently confirmed: parent
terminal cancelled; all 257 jobs terminal, with 253 cancelled, three failed and
only the planner successful. Transient HTTP502 errors on 100-job pages were
resolved by reading the same run in 50-job pages; no observation error was
treated as terminal or used to restart a run.

The [493-member failure/cancellation archive](failed-and-cancelled/manifest.json)
retains both raw JSON/report uploads, logs, artifact metadata, actual rejection
replays, pre/post-cancellation API observations and the complete final census.
Every member reopens and SHA-verifies. The separate uploaded original-store
archives 11288010719 (427,072,462 bytes) and 11287642332 (108,342,716 bytes)
are identified in the metadata but were not downloaded or independently
hash-verified here; this failure diagnosis does not claim that extra proof.
The [original first failure archive](../worker-clock-replica-capture-2026-10-03/)
remains unchanged. No rejected case or old parent is promoted.

## Full replacement dispatched

[Run 37164231641](https://github.com/AntPAllen/js-wf/actions/runs/37164231641)
uses exact corrected source `79915ca41a5c5a23b9997eea5f3f66d82530ee30`.
It requests all 16 rows × seeds 1–200 × ten minutes: 3,200 actual executions
in 256 thirteen-seed shards, four parallel. Forced upgrade Start gaps,
SIGKILL shutdown, admitted positive timer cuts and five common-clock probes
remain mandatory. Planner job 111323760432 is observed running at the matching
head. The [dispatch originals and enumerated plan](replacement-dispatch/)
have per-file readback hashes; dispatch clears no gate.

The healthy Tier2 campaign is unchanged. The historical 30-second-TTL
worker-kill mismatch is not rerun. Full current-source matrix/200-seed coverage,
the required 24-hour full-matrix soak, separate Lame Duck profile and other
original-plan acceptance requirements remain open. Five containers remain a
permitted Tier3 deployment under the supplied plan; a separate five-VM gate
is not added. Root disk capacity still prevents the local 24-hour soak.
