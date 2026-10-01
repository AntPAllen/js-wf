# Controlled lease acquisition/read/renewal under disk delay — October 1, 2026

A new opt-in Linux API-level contract runs the production lease store on one
initialized lease in a real file-backed R3 bucket. There is one fixed stream
leader throughout all rows. Eight rejected acquisitions, eight reads and eight
confirmed renewals use the same owner and 85 ms delay on that leader's existing
JetStream files. Reads/contender metadata use a pinned leader connection, avoiding
replica visibility as an apparent ownership change.

Each row records timestamps and a complete byte/hash comparison of that leader's
Raft WAL blocks. Acquisitions must return ErrHeld; reads/rejections leave owner,
revision and WAL unchanged. Renewals retain the same owner/epoch bytes but advance
revision and WAL size/hash. The renewal row must meet its sequential 8×85 ms
minimum. The stopped tracer must contain delayed calls and the selected Raft group.
This positive control confirms real durable writes and active delay injection.
A final acknowledged release removes the key.

The custom LEASE_PROBE bucket has one-minute TTL to exclude expiry from this
isolated experiment; production's 12-second lease configuration is unchanged.
The fixture uses the same production Acquire/Renew/Release code. It is not a
worker-kill, stale-reader, orphan-initialization or TTL-boundary proof.

## Results

Final race run passed in 5.619 seconds:

| Row | Calls | Duration | Leader Raft bytes |
| --- | --- | --- | --- |
| Rejected production acquisition | 8 | 17.214 ms | 640 → 640, hash unchanged |
| Held-owner reads | 8 | 3.400 ms | 640 → 640, hash unchanged |
| Confirmed production renewal | 8 | 1.046 s | 640 → 2632, hash changed |

A compiled overlay making Renew return success without updating KV fails the
WAL positive-control assertion in 2.019 seconds. It compiles and reaches the
selected behavioral assertion. The overlay never changes worktree runtime code.
Vet passes. Reports, trace, mutation, patch and source hashes are retained here.

The separate lease-disk-contract job in tier2-mixed runs this exact fixture under
race on its own hosted runner and retains reports even on failure. Routine local
suites skip it unless WF_LEASE_DISK_CONTRACT=1. Hosted confirmation is pending.

This rejects the specific hypothesis that initialized held-key acquisition
rejections necessarily append Raft WAL in this stable pinned-server contract.
It does not show that they consume no CPU/network work, explain mixed contention,
or establish behavior during elections/stale reads. The mixed latency failures
remain unresolved, and no read-before-create acquisition optimization or relaxed
renewal/fencing behavior is promoted. Full matrix and 24-hour soak stay open.
