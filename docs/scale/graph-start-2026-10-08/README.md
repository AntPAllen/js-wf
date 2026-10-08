# Canonical Start staging and recovery development

This component adds an explicit `CanonicalStarts` graph-store mode. Start reserves graph-owned input and pending identity in one root CAS, publishes a small write-once WF_INV pointer, binds the actual global source sequence, and enqueues only after binding. `Client.RecoverStart` resumes from retained input and metadata after reopening the store. Workers validate the exact pointer and read retained input before effects. Signal admission rejects pending starts; graph terminal, child and purge readers validate bound source identity.

The mode requires isolated or quiesced deployment. Existing v3 journal roots are rejected; no implicit import or mixed-version rollout is claimed. Default graph behavior is unchanged. Signals remain on legacy publication. Invocation discovery still depends on WF_INV, and pending-start recovery is explicit rather than a deployed catalog repair loop. Snapshots, continuation, remaining lifecycle races, runtime migration, production collection, and full original qualification gates remain open.

## Development evidence

The corrected native test passes prepared durable cuts (reserved, source committed, bound without enqueue, plus ordinary Start) on R1 and R3. Each executes a 5,242,882-byte input, confirms one effect across duplicate replay, exercises production graph purge and a retained input pin, and preserves source and logical journal high water on replacement. The replacement terminal is manually prepared; these are durable-state restart controls, not process-kill tests.

The first draft failed to compile because it used a nonexistent metrics field. The next draft passed all eight scenario cases but failed an incorrect assertion requiring zero physical bucket messages. Native deletion deliberately retains attempt tombstones to fence delayed publishers. The corrected assertion requires zero chunks and verifies every retained metadata record is deleted with zero size/chunks and empty digest. Original failures are retained in `development/` without changing their verdicts. This does not close the original physical million-scale drain gate or qualify permanent metadata capacity.

A 1,000-schedule development campaign covers all 18 Start modes, with production modeled dispatch/replay and independent graph reference checks. All 670 previously committed pins remain byte-identical; 18 Start pins extend the corpus to 688. The development selected-package command accidentally selected no journal/client/retention tests, so it is not package qualification. Frozen verification below must explicitly run full client and retention packages, journal prefix groups, six worker groups, and seeded Start plus the complete corpus.

## Qualification scope

Pending. Source will be committed and frozen before normal/race verification. This component is not acceptance of the full implementation plan or the original all-family 100,000-seed campaign.
