# Bounded final compaction verification at the original head

`BeginCompactionCommit` copies an original-head prepared plan, observes canonical
authority and checks the forest set, preserved readers and inherited archive
frontiers. `Advance(ctx, maxRecords, maxNodes)` separately limits newly compared
source/target records and freshly authenticated target node visits. Every record
retains data/payload equality, original payload grant and relocated payload grant
checks. Every new node retains its ready/origin/destination/location/height/stream
grant checks against exact authenticated coordinates. The walk still validates
inherited archive nodes; their frontier membership was checked at construction.

The node iterator retains only pending tree coordinates, with logarithmic space
and no node-byte cache. A failed deadline visit is not counted or removed; retry
freshly reloads it. Record comparison retains only fully checked private progress
and creates fresh range iterators per batch. Ancestor and payload-origin path
reads can recur; the budgets do not count every RPC or bound callback wall time.
Constructor inherited-frontier/path checks have no record budget. Caller contexts
remain necessary. A normal131-record walk visits259 nodes with259 GETs and at
most10 pending coordinates. An interrupted visit adds one fresh retry GET.

Partial verification returns no published root. Source authority is observed
between batches, and final CAS always uses the original head. Protocol-conforming
collection must fence that head before revoking pending grants. This is the same
collection exclusion used by the preceding synchronous verifier, spread across
private batches; it is not a cache of mutable blob authority or a portable
verified-prefix certificate. A changed head rejects progress or the final CAS.
Finalization errors, including unknown replies, latch failure. Fresh operations
can confirm an exact already committed publication. Caller edits of the input
plan or returned result do not mutate private verification state.

`CommitPrefixCompaction` preserves its synchronous behavior through one complete
batch. No root/fence schema, intent expiry, journal admission or worker timeout
changes here. Verification progress cannot be serialized and is lost on process
death. Portable **staging input** remains independently verified on commit.

## Development evidence

Fifteen commit controls cover normal/inherited archive, record/node deadlines,
append/reader/retire races, collection during record/node verification and at
final CAS, an unchecked node grant revoked independently of source authority,
dropped/lost/unconfirmed final acknowledgement, and caller plan ownership.
They pass under race. A12-record relocation compares all12 records and validates
all19 target nodes in small batches. Record deadline progress retains next4;
node deadline progress retains its completed visit and retries the failed node.
Dropped/unconfirmed finalization cannot succeed by reusing the old operation.

The full prefix-compaction selection plus updated native R1/R3 restart fixture
passes under race47.045s. The native cases now verify one record and one node per
batch, independently observing unchanged authority on every partial call. Both
complete four records/six nodes, preserve2 archive/2 live records, reclaim11
original objects and verify surviving data/payload bytes after same-store
embedded-server restart. This is component evidence, not OS process SIGKILL,
VM power loss, storage-cache loss or frozen whole-plan qualification.

The iterator's five normal/deadline/cancel/corruption/visitor controls pass under
race1.313s. All853 saved simulation regressions pass unchanged13.454s; no traces
were refreshed. Native R1/archive20 plus seven handoff repair controls passes
under race25.372s, with two checkpoints, terminal slot19 and zero forbidden
effects. The worker still uses the synchronous wrapper in those runtime checks.

Four deliberate bypasses have retained exact sources, commands and output:

- Removing the record budget fails all15 commit cases.
- Removing the node budget fails11; the four early source-change cases pass.
- Removing node grant checks fails the node-grant case and its node-deadline
  fault-sensitivity sibling; the other13 pass.
- Retrying final CAS at a refreshed head fails precisely collector-at-cas:
  it publishes after collection invalidated the already verified target grants.
  The captured-head implementation rejects this publication.

These are deliberately unsafe implementations, not historical defect claims.
Run `python3 docs/scale/graph-compaction-commit-batches-2026-10-10/review.py`
to verify fixed relevant source hashes and retained controls. Prior staging
evidence is frozen at its own commit; the changed native fixture has new evidence
here rather than retroactively qualifying its earlier source hashes.

## Still required

Worker maintenance integration, runtime staging descriptor binding/persistence,
durable checkpoint verification and intent lifetime handling remain required.
The new bounded verifier is process-local; final archive publication is still
synchronous in current journal/worker use under the unchanged15-second context.
Actual100000 remains failed. Every original full/extended/native/fault/scale/soak/
retention/import/admission/collector/rollout requirement remains open. Public
continuation admission stays closed, production collection stays off, and the
older158-family sharded race job stays on its unchanged source.
