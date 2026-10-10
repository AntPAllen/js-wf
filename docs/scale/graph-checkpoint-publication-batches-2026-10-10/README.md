# Bounded verification integrated with checkpoint publication

`GraphStore.BeginCheckpointPublication` owns a freshly acquired exact reader
and private process-local verification state. `Advance(ctx, maxRecords)` limits
new logical records in each batch. Verification cancellation/deadlines retain
completed progress while the reader remains alive. Final frame ownership has an
additional anchor read; the record limit does not bound every native operation
or guarantee a wall-clock completion time. The scan retains its parser and the
necessary latest-checkpoint suffix, whose storage can still grow.

A partial scan cannot publish a canonical pointer. Complete verification must
match the requested runtime and captured tail, then confirm reader release.
Publication subsequently observes fresh authority and fences invocation,
lifecycle, logical tail, pointer ordering and the exact root CAS. Unconfirmed
release or failed/unknown finalization latches failure; repeating `Advance` on
that operation cannot turn it into success. Abandonment requires `Close`.
A fresh operation reconciles an unknown pointer write. A release that actually
committed but could not be confirmed can return revoked on subsequent cleanup;
the tests independently inspect canonical authority rather than claiming that
the original operation confirmed release.

The existing synchronous `PublishCheckpoint` uses this operation in one batch.
Worker publication still has its 15-second context. Archive confirmation keeps
its independent fresh verification; this does not reuse a private scan result
as an archive certificate. No durable progress, maintenance dispatch loop,
archive staging change, schema change or public continuation admission is added.

## Directed evidence against the final development source

Thirty seeded in-memory controls cover JSON/protobuf × normal, deadline pause,
renewal, changed tail, tail race during pointer CAS, wrong runtime, wrong tail,
close, expiry, and three acknowledgement outcomes for each of reader release
and pointer CAS. Drop-before-commit and lost-acknowledgement cases explicitly
separate the original operation from its fresh retry. Closed/expired operations
perform no additional entry reads and leave authority unchanged. Abandoned
operations have no remaining reader in independently observed authority.

Each complete scan reads 36 logical entries plus one final anchor recheck.
The deadline case includes one failed attempt, retains next index 8 and finishes
with 38 attempts, without rescanning verified records. A four-second reader
finishes 37 virtual seconds with 18 renewals. No partial pointer is observed.

The final checkpoint/frame/index/archive selection passes under race in
136.092 seconds, including all 30 new cases and existing scan/header controls.
All 853 saved simulation regressions pass unchanged in 25.088 seconds.
Native R1/archive with private budget 20 plus directed handoff repair passes
under race in 33.309 seconds: two checkpoints, terminal slot 19, zero forbidden
effects; the recovery model reclaims 19 original receipts before stage entry.
These are development checks, not frozen current-source qualification.

Removing release confirmation fails exactly four cases (two encodings × dropped
or unconfirmed release); the other 26 pass. Bypassing the batch limit fails all
30 at the first partial batch. Exact mutant sources, commands and output are
retained; these are deliberate bypasses, not historical defect reproductions.
The initial 30-case run and initial index integration run are retained separately.
An initial mistyped native selection ran zero tests; its output is retained and
excluded from acceptance. The corrected selection supplies the native evidence.

Run `python3 docs/scale/graph-checkpoint-publication-batches-2026-10-10/review.py`
to check fixed source hashes and retained output. The reviewer refuses changed
inputs; later changes require their own evidence.

## Still required

The actual 100,000-entry native gate remains failed. Worker use remains
synchronous; process death loses scan progress. Durable handover, bounded archive
staging and reader/intent lifetime control remain necessary. Both older live
qualification jobs keep their original sources, including the previously found
node-grant validation defect; they cannot qualify this source. All original
full/extended/native/fault/scale/soak/retention/import/admission/collector/rollout
gates remain in scope. Public continuation admission stays closed and production
collection stays off.
