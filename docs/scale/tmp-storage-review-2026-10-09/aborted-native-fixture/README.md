# Follow-up `/tmp` cleanup

Archived and removed the closed NATS fixture left by the aborted
`TestNativeGraphContinuationSDKFlow/R3Domain` run and the closed development
failure trace in `js-wf-seed-failure-1691841028`. No test verdict changed.

Privileged checks found no process arguments, environments, working directories,
open descriptors, container mounts, loop devices or mounts using either root.
The S3 archive was downloaded in full and its compressed SHA-256 and every
regular file's bytes and mode were verified before deleting the originals.
Original per-file modification times are retained in the inventory.

- Reclaimed original allocation: 15,536,128 bytes (14.8 MiB).
- `/tmp`: 22,052 KiB before; 6,880 KiB after.
- S3 bucket: `nameless-bird-8772`.
- S3 key: `js-wf/tmp-cleanup/2026-10-09/54f2a6d443ffc753fdfd42417044a9639e43f3791a148c4f7beee0b62bf8ec85.tar.gz`.
- Compressed archive: 696,042 bytes.

[removal.json](removal.json) records the closure checks and readback hash;
[fixture-inventory.json](fixture-inventory.json) records each archived file.

The remaining `/tmp` allocation is small: approximately 4.5 MiB of other tool
state, 0.8 MiB in a registered source worktree, plus small worktree remnants,
scripts, attachments and system/tool directories. These were retained.
`/var/tmp` uses 20 KiB, and the privileged deleted-open-file check found no
entries. The root filesystem has approximately 50 GiB available.
