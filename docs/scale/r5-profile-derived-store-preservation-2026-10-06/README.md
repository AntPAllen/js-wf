# Lossless current R5 diagnostic clone preservation

The closed derived root `/tmp/js-wf-r5-retained-profile-restored-20261005` was
preserved before any copy reclamation. Its earlier profile archive retained
post-state hashes, not all current copied store bytes; that earlier record alone
was insufficient to remove the copy. This supplement captures the complete
current root, including all 640 copied-store files, sources, executables,
original metadata, initial/continued diagnostic logs and closure checks.

The original recorded SDK PID 225923 and five inspected server PIDs are absent;
all five historical Docker containers were removed. Current running Docker mounts
and visible task descriptors do not reference this root. Descriptor permission
limits are recorded explicitly. These checks describe observed closure, not an
exhaustive lifetime record of every subsequent diagnostic process.

The canonical base is the full `short-success-r5-failed` archive, pinned at
`e222741`, SHA256 `f6789ee4adcdbea3323ac91c401198cac7c77e9ab21ba96de4afa091b9643c2f`.
All base parts/members/embedded manifest were read and verified. Exactly 422
unchanged store files / 856,159,377 bytes reference matching base members.
All other current bytes are physically retained in the delta: **99,772,923 bytes /
four parts / 893 physical members**. Base plus delta reconstructs **1,314 logical
files**, with SHA256, sizes, modes and mtimes in `lossless-manifest.json`.
The full virtual tree was independently verified twice without restoring or
opening any NATS store. Both base and delta are required for reconstruction.

`preparation.json` retains the complete current store inventory and observed
closure/mount/descriptor checks. Capture did not modify store bytes or reopen NATS.
Reclamation remains pending a pushed canonical delta and a final verification.
Earlier failed/successful diagnostic verdicts and their source-capture limits
remain as documented; this preservation does not qualify a runtime/release gate.
The original donor and failed native fixture remain untouched.
