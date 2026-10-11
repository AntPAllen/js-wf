# Closed original100000-entry store archive — 2026-10-11

The original6408a5 normal completed with actual supervisor/child exit0, but full
acceptance remains false because41 CASRoot errors were unclassified. This
storage operation does not alter that verdict or qualify a race run.

The exact user unit is terminal with MainPID0 and invocation
290b17b6df9e45629c853c10ed5cc772. The child PID is absent, visible process handles
are closed, and root lsof found no open store files. Before capture, all3170
native files and3369550584 bytes matched the closed run's original SHA256 census.
Capture then verified every archive member and every unchanged original file.
Root state/review hashes are unchanged; original logs/manifests remain local and
in the existing campaign evidence directory.

The lossless archive has671132643 compressed bytes. The complete file inventory,
archive hash, closure receipts and executed capture script are committed here.
Upload/readback, fresh recovery and removal are pending at this preparation
checkpoint. No local files have been removed. All current live stores and
qualification worktrees remain local.

The archive captures regular-file bytes, paths, modes and nanosecond mtimes;
owner/directory metadata and runtime recoverability are separate claims.

## Completed upload, recovery and removal

The complete compressed body, metadata and inventory were uploaded to the user's
S3 endpoint and read back byte-for-byte. Fresh local recovery restored every3170
file and verified bytes, SHA256, mode and nanosecond mtime. The original campaign
reviewer independently checked all restored files plus frozen inputs, binary,
actual unit/child completion and full functional assertions. Its verdict remains
accepted=false because the same41 errors are unclassified. Separate output
preserves the original review receipt unchanged.

Upload/recovery receipts were committed and pushed in a42cce9 before removal.
Removal then rechecked the committed proof, both complete file inventories,
archive members, actual closure, root lsof and original state/review hashes.
The ledger records every removed path and its inode/device/link/allocation data.
The original3170 store files released3375587328 allocated bytes (3.14GiB).
Task-created archive/recovery copies were removed separately and are excluded
from that original-data reclaimed total. Free space afterward was8718389248
bytes (8.12GiB), while both live qualification services remained running.

Original logs, source manifests, file census, verdict and loaded terminal unit
remain local; the root contains native-storage-archive.json pointing here.
Native data now lives in the content-addressed S3 archive. No live store,
qualification checkout, executable, cache or user attachment was removed.

## Restore to a fresh directory

The following uses the user-provided x/x credentials through curl stdin config.
Choose unused archive/destination paths. The restore command verifies the
complete archive hash and embedded file census before writing a fresh tree.

```sh
curl --config - --aws-sigv4 aws:amz:us-east-1:s3 --fail --output /home/exedev/entry100000-proof.tar.gz 'https://nameless-bird-8772.int.exe.xyz/nameless-bird-8772/js-wf/proofs/b74da52ee7ec0351b74a6dde02a0db4203884518975372c1e97f912e6ca56f36/proof.tar.gz' <<'Chttps://nameless-bird-8772.int.exe.xyz/nameless-bird-8772/js-wf/proofs/b74da52ee7ec0351b74a6dde02a0db4203884518975372c1e97f912e6ca56f36/proof.tar.gz'
user = "x:x"
Chttps://nameless-bird-8772.int.exe.xyz/nameless-bird-8772/js-wf/proofs/b74da52ee7ec0351b74a6dde02a0db4203884518975372c1e97f912e6ca56f36/proof.tar.gz
python3 scripts/restore-full-fixture-proof.py --archive /home/exedev/entry100000-proof.tar.gz --metadata docs/scale/closed-entry100000-storage-2026-10-11/archive-verification.json --inventory docs/scale/closed-entry100000-storage-2026-10-11/fixture-inventory.json --destination /home/exedev/restored-entry100000-native
python3 docs/scale/graph-indexed-entry-campaign-2026-10-10/review.py /home/exedev/js-wf-indexed-entry100000-normal-20261010 --native-root /home/exedev/restored-entry100000-native --output /home/exedev/restored-entry100000-review.json
```

The reviewer command requires this VM's original unit receipts, frozen checkout,
binary and root logs. Fresh file recovery is independently possible without
those prerequisites. Recovery does not start NATS or change a test verdict.
