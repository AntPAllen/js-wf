# Full-cohort R5 byte-bounded audit comparison

The original copied cohort contains 100,000 completed invocations and 1,200,000
journal records. Before this run, all 655 clone files match the published
`r5-audit-profile-2026-10-05` archive's
`attempt-2/post-copied-store-hashes.json` (the clone also served attempt 3).
The copied stores continue under the original logical NATS identities with fresh
Docker names. Original failed-campaign stores are not used or modified.

Normal executable, GOMAXPROCS=2, GOMEMLIMIT=2GiB, five file replicas, explicit
route seeds and original 20-second per-attempt deadline. State snapshot and
streaming invariant checks are the same for all four measurements. Each cursor
is verified through consumer Info to retain R5 replication.

| Variant | Elapsed | Complete report |
| --- | --- | --- |
| R5 / 512-record Fetch | 20.000422175 s | No: deadline during final state checks |
| R5 / 4096-record byte-bounded iterator | 17.098872702 s | Yes: 100k invocations/journals/terminals, 1.2M entries |
| Byte-bounded recheck | 15.209135208 s | Yes: same exact report |
| 512-record Fetch recheck | 20.001064114 s | No: deadline |

Both baseline attempts delivered all 1.2M journal records; this is still
insufficient for a passing audit because final state validation must also finish.
The first baseline's `terminal state missing: context deadline exceeded` is a
deadline error, not evidence that the persisted state was absent. Complete
candidate reports check that state. No partial report is promoted.

The named diagnostic passes in 129.41 s, including startup; its PASS means the
comparison finished and recorded each verdict. There are no admitted fault cuts
in this run. This is a small-payload cohort (84.7MB journal data), not a replacement
for the separate refill-sized payload control or interrupted-window acceptance.
Two sequential measurements do not establish a stable production speedup.

## Evidence and next gate

Live `/proc/PID/exe` hash, full SDK build information, all five actual container
binary copies/build information/inspect records, 639 selected Go/module sources
and matching before/after hashes, producer script, logs, four CPU profiles and
phase reports are archived. This is not an exhaustive compiler-input inventory.
Copied-store continuation and post-run ledgers are included; the clone bytes
remain at the recorded VM path and are not duplicated in this archive. Original
input store bytes are preserved by the earlier failed-campaign archive.

The first candidate profile samples 10.16 CPU seconds over 17.10 wall seconds;
allocation/GC and NATS delivery remain material. Profiles are diagnostic and do
not alone establish causality. The SDK's byte-mode pointer-channel allocation
limitation remains documented in the parent evidence.

Next: validate both explicit-error and short-success interrupted windows on the
same full R5 cohort under the original deadline, then make a controlled adoption
decision. Production defaults, original limits, final-source matrix and actual
24-hour qualification remain unchanged/open.
