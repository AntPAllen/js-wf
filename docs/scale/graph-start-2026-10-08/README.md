# Canonical Start staging and recovery verification

This component adds an explicit `CanonicalStarts` graph-store mode. Start reserves graph-owned input and pending identity in one root CAS, publishes a small write-once WF_INV pointer, binds the actual global source sequence, and enqueues only after binding. `Client.RecoverStart` resumes from retained input and metadata after reopening the store. Workers validate the exact pointer and read retained input before effects. Signal admission rejects pending starts; graph terminal, child and purge readers validate bound source identity.

The mode requires isolated or quiesced deployment. Existing v3 journal roots are rejected; no implicit import or mixed-version rollout is claimed. Default graph behavior is unchanged. Signals remain on legacy publication. Invocation discovery still depends on WF_INV, and pending-start recovery is explicit rather than a deployed catalog repair loop. Snapshots, continuation, remaining lifecycle races, runtime migration, production collection, and full original qualification gates remain open.

## Development evidence

The corrected native test passes prepared durable cuts (reserved, source committed, bound without enqueue, plus ordinary Start) on R1 and R3. Each executes a 5,242,882-byte input, confirms one effect across duplicate replay, exercises production graph purge and a retained input pin, and preserves source and logical journal high water on replacement. The replacement terminal is manually prepared; these are durable-state restart controls, not process-kill tests.

The first draft failed to compile because it used a nonexistent metrics field. The next draft passed all eight scenario cases but failed an incorrect assertion requiring zero physical bucket messages. Native deletion deliberately retains attempt tombstones to fence delayed publishers. The corrected assertion requires zero chunks and verifies every retained metadata record is deleted with zero size/chunks and empty digest. Original failures are retained in `development/` without changing their verdicts. This does not close the original physical million-scale drain gate or qualify permanent metadata capacity.

A 1,000-schedule development campaign covers all 18 Start modes, with production modeled dispatch/replay and independent graph reference checks. All 670 previously committed pins remain byte-identical; 18 Start pins extend the corpus to 688. The development selected-package command accidentally selected no journal/client/retention tests, so it is not package qualification. Frozen verification below must explicitly run full client and retention packages, journal prefix groups, six worker groups, and seeded Start plus the complete corpus.

## Qualification scope

Frozen source `1c777bb21af7972f7dd0e309c6fa4ab170d04d33` passes all ten commands under the predetermined five-minute/count1/two-Go-CPU/512MiB envelopes. Independent review verifies 1,627 selected inputs against Git and unchanged before/after/current bytes, compiled test inventories, native R1/R3 scenario events, all 18 mode counts, actual completed seeded bodies, every pin, and the 18-row CI family inventory.

| Suite | Normal seconds | Race seconds |
| --- | ---: | ---: |
| Full client (5 groups) | 4.751 | 15.285 |
| Graph journal (16 groups) | 7.016 | 46.637 |
| Graph worker (6 groups) | 31.052 | 168.801 |
| Retention (9 executed groups) | 9.250 | 30.107 |
| Start + corpus + independent census (4 groups) | 100.750 | 164.288 |

Start completes 10,000 normal / 1,000 race schedules, each generated and exactly replayed, with all 18 modes exercised. Both commands replay all 688 pins; all 670 older pins remain byte-identical. The optional `TestBlobSweepConcurrentRefreshContract` requires a fresh artifact directory and is skipped in both retention commands; it is not newly qualified here. The new native worker test exercises actual canonical Start purge in both modes.

[Independent review](review.json), [exact commands/results](results.json), [executed runner](executed-regression.py), and [executed reviewer](executed-review.py) retain the evidence. This component does not accept the full implementation plan or the original all-family 100,000-seed campaign. No hosted CI success is inferred.
