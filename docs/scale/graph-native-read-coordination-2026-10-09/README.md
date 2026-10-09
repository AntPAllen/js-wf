# Native authority read coordination — 2026-10-09

A new real R1/R3 control issues 32 concurrent reads of one stable authority, with no logical writer. Before the change, both replica cases return definite witness CAS conflicts; the R3 cohort has 14 failed readers. Successful reads still preserve logical bytes, so this is observed adapter self-contention rather than evidence of a NATS storage defect.

Native authority now serializes complete read-witness operations per kind/identity on one adapter. Each read still obtains a fresh snapshot and an actual quorum-acknowledged conditional witness; no cached data or weaker observation replaces it. Mutations retain server CAS arbitration and do not take this gate. The gate observes caller cancellation. Its entries are removed when the last active/queued read finishes, bounding retained local state by concurrent reads rather than historical identities. Adapter copies share a coordinator pointer without copying its mutex.

The new cohort passes with all 32 readers and exactly 32 acknowledged physical witnesses in each replica case, with unchanged logical bytes. A held-witness control confirms that queued cancellation does not release the active read, a mutation and independent subject still complete, the stale witness reloads the committed replacement, and no inactive coordination entries remain.

## Qualification

The complete graph authority package passes under race in 179.702 seconds, including malformed metadata, witness and mutation reply uncertainty, permissions, native publisher/collector interleavings, reader expiry and store restart. The initial cohort and focused controls used a value-field coordinator and are retained as development evidence; the final implementation uses a pointer so existing adapter copies remain safe. `source-observed.json` identifies final repository Go/module inputs observed during the package race launch.

Worker normal/race qualification uses the same six v4/v5/v6 × R1/R3-domain ordinary workflow cases with GOMAXPROCS=2 and GOMEMLIMIT=512MiB. All six cases pass normally (48.909 s) and under race (243.448 s). Its original two-minute per-fixture deadlines remain unchanged. The earlier failed combined CLI campaign used the VM's default four Go CPUs; its failure cannot be attributed exclusively to this change from differently configured retests. A matching default-concurrency campaign is recorded separately.

Complete current 150-family/extended qualification, independent frozen fca8264 full race, original native fault matrices, autonomous collection, staged continuation CLI/admission, import/deployment, scale/actual-soak/drain and release requirements remain separate. Public continuation admission and production collection remain disabled.
