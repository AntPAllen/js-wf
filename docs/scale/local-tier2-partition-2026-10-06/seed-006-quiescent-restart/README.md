# Seed6 fresh-copy quiescent restart diagnostic

Prepared diagnostic downloads the committed S3 full failed-seed archive, verifies every member and the complete compressed digest, restores to a fresh baseline, and creates a separate byte/mode/mtime-matching mutable cluster copy. Three real NATS processes use the exact captured seed6 executable bytes. No workflow writers or fault injection run.

A fixed120s observation checks leader-reported current/online status of all ten workflow stores and records each actual peer's public local `/jsz` details. This separates post-restart quiescent recovery from the original running-workload partition failure; success cannot qualify the native row or establish its cause. Complete restored baseline bytes and selected helper/runtime/external sources are compared after shutdown. No original historical broker is opened.

Command:

    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x python3 scripts/run-partition-recovery-diagnostic.py --root /tmp/js-wf-partition-seed6-quiescent-20261006 --failure-proof docs/scale/local-tier2-partition-2026-10-06/seed-006-failure

Helper compile passed. Actual diagnostic remains pending.
