# Closed accepted 100000-entry normal store archive — 2026-10-11

Completed: the closed server media now lives in the fully verified S3 archive.
Fresh recovery passed the original acceptance gates. All 3170 original store files
and this task's temporary copies were removed after proof commit 154dbbd was pushed.
Original final-link allocation removed: 3,371,347,968 bytes (3.14GiB). Free space
at completion: 9,160,499,200 bytes (8.53GiB). All three live jobs remain running.

The accepted classified normal source 9189259 completed with actual child and
supervisor exits 0 under InvocationID 4574548accd94da986ff05b3652910d2. Its exact
loaded service remains MainPID=0/RemainAfterExit=yes. Root lsof and visible process
handles confirm closure. All 3170 native files/3,365,343,276 bytes match the original
closed file census. Capture verifies every archive member and every unchanged
original file, including modes and nanosecond mtimes. Original state/review hashes
are unchanged. Original logs/manifests and the accepted verdict remain local.

Archive SHA256: c27428cab667d7144d3eef26e3d820ae3777f4c5a20ab6c91f756b84a9e5dd8d.
Compressed bytes: 670940778. Metadata and complete inventory are committed here;
archive staging remains outside Git. Upload/readback, fresh recovery, independent
review and deletion are pending at this preparation checkpoint. No local stores
have been removed. The three live campaigns retain their own stores and inputs.

The classified reviewer now accepts --native-root and --output so fresh recovery
can be checked against the original complete census without overwriting the
original verdict. All loaded unit/source/binary/full-log/error-classification gates
remain required. This moves closed server media; it does not change normal
acceptance, qualify the live race or claim a successful NATS restart/power loss.

The acceptance remains true with 57 exact definite CAS conflicts and no uncertain/
unclassified errors. Zero-profile-error status remains false. Original broader
scale/fault/soak/rollout/admission/import/online-collection gates remain open.

## Completed remote readback and fresh recovery

The archive, canonical metadata and inventory have been uploaded and fully read
back from the user-provided S3 endpoint. A separate fresh download matched the
entire compressed-body hash before replacing this task's staging archive.
Recovery restored all 3170 files/3,365,343,276 bytes and verified bytes, modes and
nanosecond mtimes. The independent classified reviewer reran every existing
unit/source/binary/log/error-classification/file-census gate against the recovered
tree with separate output: accepted=true, actual 100000 entries qualified=true,
57 definite CAS conflicts and no unexpected errors. The original root verdict
and state remain byte-for-byte unchanged.

The complete remote and restoration receipts are committed before deletion.
Original store removal and task-created archive/recovery cleanup remain pending
at this checkpoint. Restoring to a fresh directory requires the committed
metadata/inventory and this archive object:

js-wf/proofs/c27428cab667d7144d3eef26e3d820ae3777f4c5a20ab6c91f756b84a9e5dd8d/proof.tar.gz

S3 endpoint https://nameless-bird-8772.int.exe.xyz; bucket nameless-bird-8772.
Use the user-provided credentials through curl stdin configuration, then:

```sh
python3 scripts/restore-full-fixture-proof.py --archive /home/exedev/downloaded-classified-proof.tar.gz --metadata docs/scale/closed-classified-entry100000-storage-2026-10-11/archive-verification.json --inventory docs/scale/closed-classified-entry100000-storage-2026-10-11/fixture-inventory.json --destination /home/exedev/restored-classified-entry100000
python3 docs/scale/graph-classified-entry-campaign-2026-10-11/review.py /home/exedev/js-wf-classified-entry100000-normal-20261011 --native-root /home/exedev/restored-classified-entry100000 --output /home/exedev/restored-classified-entry100000-review.json
```

The second command uses this VM's original loaded unit, frozen checkout, retained
binary and root logs. Fresh file recovery needs only the downloaded archive and
committed metadata/inventory. The restore does not start a server.

## Completed removal

Removal rechecked the committed remote/recovery proof, entire archive, both full
file inventories, actual closure/root lsof and unchanged original state/review.
The ledger records each removed file's path, size, device, inode, link count and
allocated blocks. Temporary archive/recovery removal is recorded separately from
the original 3.14GiB reclaimed data. Original source manifests, log, census, verdict
and loaded terminal service remain local. The root sidecar points to the S3
archive and restoration instructions; a copy is retained here.
