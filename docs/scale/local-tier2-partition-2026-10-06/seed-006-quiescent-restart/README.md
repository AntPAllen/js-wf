# Seed6 fresh-copy quiescent restart diagnostic

Prepared diagnostic downloads the committed S3 full failed-seed archive, verifies every member and the complete compressed digest, restores to a fresh baseline, and creates a separate byte/mode/mtime-matching mutable cluster copy. Three real NATS processes use the exact captured seed6 executable bytes. No workflow writers or fault injection run.

A fixed120s observation checks leader-reported current/online status of all ten workflow stores and records each actual peer's public local `/jsz` details. This separates post-restart quiescent recovery from the original running-workload partition failure; success cannot qualify the native row or establish its cause. Complete restored baseline bytes and selected helper/runtime/external sources are compared after shutdown. No original historical broker is opened.

Command:

    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x python3 scripts/run-partition-recovery-diagnostic.py --root /tmp/js-wf-partition-seed6-quiescent-20261006 --failure-proof docs/scale/local-tier2-partition-2026-10-06/seed-006-failure

Helper compile passed. Actual diagnostic remains pending.

## Independently reviewed result

At executed623ab82, the corrected fresh S3 restore/copy diagnostic passes: all ten workflow stores have current, online replicas after10.336s, in three observation rounds. Three live `/proc` captured NATS executables exactly match the original failed seed SHA24759ea..., with actual node/store argv and birth records. All three public local `/jsz` responses identify the three actual peers and contain the ten store names. Independent review binds1944 Git/before/after/current/retained source inputs,1494 external inputs, unchanged complete restored baseline and matching initial mutable copies. Complete11948member archive is read back and compared. [Corrected proof](corrected/independent-review.json).

This shows the captured stores can recover after a quiescent full restart; it does not prove the cause of their earlier catchup stall under a live route partition and lease writes. The original seed6 and200 row remain failed, and a causal Tier1 reproduction remains open. No production restart workaround or NATS defect claim is made. Initial observer-layout failure is separately retained and fully stored in S3.
