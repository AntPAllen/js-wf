# Durable journal compaction descriptors — 2026-10-10

`GraphConfig` and `NativeGraphConfig` can select a dedicated descriptor KV port.
`CheckpointCompaction.SaveCheckpoint` creates at expected revision zero or updates
exactly the supplied revision. `ResumeStoredCheckpointCompaction` loads staging
input, checks identity/source binding and intent expiry, and reconstructs a fresh
operation. Missing storage is reported separately from corrupt/stale/expired input;
no failed read silently starts another operation. Every record/node/grant still
requires the private final verification and original-head CAS. Saved verification
progress is discarded. `DeleteCompactionCheckpoint` uses exact revision CAS.

Unknown create/update/delete replies return ErrUnknown without retries or readback.
A fresh attempt reloads whichever bytes committed. Known CAS conflicts return
ErrStale. Keys hash the complete type/ID/runtime checkpoint/tail binding. KV
revisions provide storage concurrency control, not a lease or content proof. The
caller must renew its owner before each descriptor/grant/publication mutation.
The bucket is separately provisioned; select file storage, matching replicas,
no TTL, history1 and MaxValueSize at least536576 bytes for this envelope bound.
Its native adapter uses only public JetStream KV methods and LastRevision delete.

## Evidence

36 JSON/protobuf storage controls pass under race7.869s after restoring four
bypasses. They cover healthy/missing/canceled input, dropped/lost create/update/
delete replies, stale writers/deletion/read revisions, lost reads, corrupt bytes,
expired grants, reader-induced source changes, saved verification progress and
pending renewal (including a grant CAS that committed without its reply). Fresh
GraphStore handles resume without old operation state. Healthy resumed paths
require all10 final verification batches. Independent canonical retention is9
only after verified publication, otherwise0; all readers are released.

Deliberately refreshing update/delete revisions fails4/2 controls; omitting expiry
fails2; retrying uncertain descriptor writes fails8. Exact bypass source bytes
are compressed alongside their logs. Production was restored before controls
were rerun. The initial development expiry check failed2 cases and is retained.

The combined journal/native race selection passes91.469s:36 storage controls,
40 existing binding controls,11 malformed envelopes and R1/R3 native domains.
Native cases persist pending-renewal input in ARCHIVE_PROGRESS file-backed KV,
restart every peer from the same stores, bind a fresh KV adapter and resume the
stored revision (not the in-memory descriptor). They retain the old reader,
renew stage/verification grants, check original-expiry sweeps, verify successive
compactions and zero retired chunks, and confirm descriptor deletion survives a
second all-peer restart. Whole-fixture2-minute deadlines are unchanged. Server
restarts are embedded graceful same-store cuts; there is no VM/power-loss claim.
The existing modeled client supplies the invocation fixture, not a native worker.

An earlier R3 development run failed after a KV metadata lookup consumed120s.
That log is retained compressed. Fresh metadata lookups now have3-second read
contexts and read-only retry within the original parent. The successful run
needed no such retries; it does not establish the former server-side cause or
clear broader real-cluster failures. Descriptor/grant mutations never retry.

All853 unchanged common pins pass9.485s. `sources.json` fixes eight relevant source
files; matching compressed bytes and `review.py` verify the evidence scope.

```sh
go test -race ./journal -run '^(TestGraphCompactionStoredCheckpointCASAndRecovery|TestGraphCompactionCheckpoint.*|TestNativeGraphCheckpointArchiveReopenAndCollection)$' -count=1 -v
go test -race ./journal -run '^TestGraphCompactionStoredCheckpointCASAndRecovery$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-compaction-durable-storage-2026-10-10/review.py
```

## Remaining runtime work

Worker deliveries still use ephemeral progress. Their reader changes the source
head on open/close, so merely attaching this port cannot enable useful recovery.
Release the delivery reader after confirmed suspension and before capturing the
compaction source. On redelivery, resume stored maintenance before opening a new
reader. Persist every completed staging batch and requested renewal expiry, with
lease checks and exact descriptor revisions; remove only confirmed obsolete input.
Bound the handoff work across deliveries without admitting a continuation early.

Public graph continuation admission remains closed, production collection remains
off, and actual100000-entry qualification remains failed. This storage milestone
does not qualify process termination during worker maintenance, changed parent
deadlines, full current seed campaigns or the original rollout gates.
