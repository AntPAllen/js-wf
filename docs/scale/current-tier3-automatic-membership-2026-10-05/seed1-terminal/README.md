# Fresh automatic-membership seed1: terminal native pass

Clean `cadb346d0eab9f6a688c52b6ef658620d4aed07e`, original R5/seed1/10m,
race/512MiB/GOMAXPROCS=2, default automatic membership and route seeding,
production two-minute sync and unchanged 20s/60s audit, 30s recovery and 5m Await.
Named test PASS 770.03 seconds (package771.113), terminal 06:02:43 UTC.
Producer completed its row, event-explanation and fencing checks, then archived
all originals and exited successfully. Result: 63 batches, 1,764 invocations,
19,427 entries, 19 actual journal faults and six completed cohort audits.
Worst terminal/progress p99 15.689s/10.203s, within original30s gates.
865 acknowledged coordinator writes, 64 partitions, five workers,36 rejoin events.

This is a fresh native and producer-checked single-seed pass. Independent replay of all2,272 history operations now passes all three exactOk
models at the executed revision; dependencies/binary/outputs are preserved in
../seed1-model-review. This accepts the focused executed-source seed1. It does not explain or
erase the older79915ca automatic-membership failures, qualify seeds2–200, or
clear the full Tier3/current-source/24h gates. Shared VM load includes the failed
journal soak and live million campaign. No failed-workload stack is expected on
this healthy test; stack-capture controls are preserved separately.

## Preservation

All5,324 canonical members independently streamed/hash-matched to the producer
ledger and every preserved physical original; source-before/after inventories
are identical. Complete producer archive SHA256
`9f9f86e3927c25c9370478f19a688fac9c35c56270e3b5d68e59e9e3510c6218`
is split into25MiB parts here, with every part/aggregate read back. It includes
actual SDK/server binaries, source, events, history, membership/fencing/checkpoint
records and closed original native stores. No stores reopened or repaired;
original root/canonical retained. Actual live identities and1,307 selected Git
inputs were verified in ../seed1-launch. Complete-source/toolchain closure is
not claimed from a selected-input inventory. See archive-verification.json.
