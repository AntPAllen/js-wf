# R5 process replay/cleanup diagnostic: failed parent preserved

Executedeb0f5ca, native race FAIL70.11s. Fresh owner-left-down audit completes
1500/6000 in original20s, then bounded cleanup observations show WF_JRN count1
through the same20s deadline. Same-store restart case passes30.40s overall
fixture; it resumes with a new R1 cursor at1962 and no overlap reaches the visitor.
This passing schedule does not explain or qualify the prior rejected same-owner
sequence1 replay. Existing strict replay admission remains unchanged.

Actual cursor Info snapshots and every cleanup observation retained. Production
cleanup uses one2s context for sequential deletions: an unanswered old-owner delete
can consume the context before replacement deletion. A changed candidate will
issue these independent deletions concurrently under the same2s ceiling/original
caller deadline and capture actual per-name delete replies. No audit budget or
retry/replay allowance is enlarged.

Independent selected Git Go/module inputs, actual race SDK/external server
bytes/module/mounts/closure and full1667-member69,244,501byte archive verified.
SHA256 `76d5368d84e3fc813e0c68d71b92b898e7454b15b17f6f3d70d29150b4edd75e`.
The initial reviewer destination collision is recorded by its systemd failure;
corrected reviewer used this new destination and completed full readback. No
production/default/large-fault/24h qualification; originals stay closed.
