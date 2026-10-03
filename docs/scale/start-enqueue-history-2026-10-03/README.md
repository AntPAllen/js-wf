# Start enqueue failure history correction

Actual failed jobs in live Tier2 campaign37057872230 at076ebad retain
`history=Illegal err=<nil>` for journal seeds39/86 and consumer seeds90/116/139.
Their original histories, test output, job logs and observed job metadata are
archived unchanged. ZIP member hashes cover the downloaded complete artifacts;
this archive retains the failed seeds' extracted originals. The full downloaded
ZIPs remain in the local evidence root.

`ErrEnqueueUnknown` confirms a stored invocation, but does not distinguish its
creator from a matching retry whose dispatch also failed. The old checker counted
both as creators. The corrected ordinary model and large-burst optimization allow
matching observations while retaining the exclusive literal `started` creator,
stable nonzero invocation sequence, matching input/parent and temporal checks.

All fifteen offline checks (Start, Signal and Result for five seeds) pass with
the correction. The production client reproduces two dispatch failures followed
by AlreadyStarted, with exactly one stored invocation and one retained dispatch.
Both drop-before-commit and first-ack-loss variants replay exactly. The original
checker fails both at `client-generated retry history=Illegal`; the corrected
checker and all269 regression pins pass race in6.172s. The history package race
suite passes including negative controls for double creation, changed identity,
zero/mismatched sequence and observation before later creation.

These fixed regressions do not increase the121 seeded workload count. No runtime
or NATS dependency changed. Offline acceptance does not qualify failed hosted
shards: they stopped before the final latency and drain gates. Full current-source
1k/100k/race qualification and the full real matrices remain open.

`manifest.json` records SHA256 for every member, verified by reopening the complete
temporary archive before atomic rename. Raw Go JSON, original/corrected checker
sources, trace bytes and the offline review utility are retained. Absolute paths
in the original overlay files document the actual execution environment; rebuild
an overlay against extracted sources to reproduce elsewhere.
