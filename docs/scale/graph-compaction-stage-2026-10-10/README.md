# Bounded archive staging with portable completed progress

`Protocol.BeginPrefixCompaction` captures an exact original source root and
publication token. `CompactionStage.Advance` stages caller-sized batches of new
logical records, without publishing authority. A record may require multiple
uploads. Between batches, source authority is observed freshly; a changed head,
reader set, forest or lifecycle rejects further progress. The final commit still
performs its full source/target comparison, independent original/relocated payload
grant checks, relocated node coordinate checks, inherited archive checks and
captured-head CAS. Commit remains synchronous.

`Checkpoint` serializes completed progress and bounded forest frontiers, with an
explicit maximum of 528384 bytes. It omits payload deduplication maps and uncertain
record uploads. `ResumePrefixCompaction` strictly decodes canonical bounded bytes
and obtains matching fresh source authority. The descriptor is staging input,
**not a verified-prefix certificate**. Structurally valid forged target data or
payload receipts still reach independent full commit verification and reject.
Application metadata retains the protocol's existing opaque caller contract;
future runtime persistence must bind the descriptor to its requested operation.
This does not make arbitrary serialized application metadata trusted.

After resumption, shared payload selections are recovered only from fresh ready
authority with matching destination/head/expiry. Existing per-stream deduplication
is recovered through exact authenticated partial-forest edges. Repeated restarts
do not consume a new grant location per batch. In-process deduplication maps still
scale with distinct payloads; serialization excludes them. Grant expiry remains
the original fixed expiry. Expired/collected plans cannot revive old receipts:
source fencing rejects stale progress and full commit rejects revoked targets.
There is no intent renewal or runtime persistence loop in this change.

## Development evidence

The final prefix-compaction selection passes under race in 19.997 seconds. It
includes 12 batch/restart/authority cases, 12 malformed/forged descriptor cases,
two freshly granted forged-prefix cases, caller result ownership, 32 sampled
random budget/cut/restart schedules, and all existing compaction/grant/content
controls. The 32 schedule samples use the local component memory port; they are
not a new common Tier 1 replay family or an extended seed qualification.

Drop-before-upload, lost upload acknowledgement and lost ready-grant
acknowledgement retain only completed records and recover through a fresh
descriptor. Append, retirement, reader acquisition and collection invalidate
resumption. Stale relocated grants cannot commit. No batch publishes a pointer
or root, and partial calls return no usable prepared plan. Caller edits of a
returned complete plan cannot mutate private staging progress.

Removing the batch limit fails all 12 batch controls. Removing fresh source
binding fails 12 controls: four changed-source cuts, seven post-commit descriptor
rejections, and the forged source descriptor; the stale-target sibling and other
11 malformed descriptor controls pass. Removing record/payload comparison fails
both newly granted forged-prefix controls. Exact mutants and output are retained.
These are deliberate guard bypasses, not historical bug reproductions.

All 853 saved simulation regressions pass unchanged in 7.004 seconds; no pin
refresh was required. Native R1/archive20 plus seven handoff recovery controls
passes under race in 20.098 seconds: two checkpoints, terminal slot 19, zero
forbidden effects, and 19 reclaimed original receipts in the recovery model.
These native runtime controls use the existing synchronous wrapper.

The new native R1/R3 staging restart fixture passes under race in 11.977 seconds.
It writes a descriptor after record 1, clears stage/adapter handles, stops all
embedded peers and restarts their existing stores, reopens authority/object
adapters and resumes in single-record batches. Both cases commit exactly two
archive/two live records, reclaim 11 original objects and verify all new metadata
and shared payload bytes. This is same-store embedded-server restart evidence,
not OS process SIGKILL, VM power loss or storage-cache-loss qualification.

The initial native R3 case timed out reopening authority, before resumption.
Its failure is retained. The fixture now explicitly waits for a connected client
and restored metadata quorum, as the existing reader restart fixture does, inside
the unchanged 45-second parent. The bounded opener and all production timeout
defaults remain unchanged; the later pass does not establish a server root cause
for the initial timeout. The initial failed row is excluded from acceptance.

Run `python3 docs/scale/graph-compaction-stage-2026-10-10/review.py` to verify
fixed relevant source hashes and retained outputs. Evidence is development scope,
not frozen complete-current-source qualification.

## Remaining work

The journal/worker still use synchronous archive preparation and commit under
the existing 15-second publication context. Staging can carry portable progress,
but the runtime does not persist or dispatch it yet. Worker maintenance integration,
durable checkpoint verification, intent lifetime control, bounded final commit
verification and the failed actual 100000-entry native gate remain required.
Every original full/extended/native/fault/scale/soak/retention/import/admission/
collector/rollout gate remains in scope. Public continuation admission stays
closed and production collection stays off. The older 158-family sharded race
job remains on its unchanged source and cannot qualify this implementation.
