# Full R5 continuous-byte reader and trace qualification

Both clean executed-source campaigns use the same verified disposable100k
completed-invocation/1.2M-entry cohort, R5 file stores and read cursors, normal
GOMAXPROCS=2/GOMEMLIMIT=2GiB, explicit route peers, production2m sync and the
original20s full-audit deadline. Original failed-campaign stores are untouched.

| Executed source | Case | Complete audit |
| --- | --- | ---: |
| ee4c272 | Continuous-byte baseline |12.501s |
| ee4c272 | Explicit-error interrupted batch |14.316s |
| ee4c272 | Short-success interrupted batch |13.692s |
|6176205 | Plain full audit |14.964s |
|6176205 | Traced full audit |14.064s |

Every audit returns exactly100k invocations/journals/terminals and1.2M entries.
The first campaign checks3.6M ordinal/exact-once journal visits. Journal cursors
start at1; explicit-error recovery starts129; short-success leader oracle reads129
then resumes130. Point reads are respectively0/0/1 and every cursor retainsR5.
The interruption hook now wraps the PRODUCTION continuous iterator's batch,
not the obsolete per-batch standalone helper. It deliberately drops one pull's
suffix after128 records; this is not a natural server/TCP fault reproduction.
Named diagnostic PASS56.00s requires all three complete audits and admissions.

The second campaign calls public streaming-state integrity through the actual
opt-in integration trace wrapper. It accounts for100k invocation Next calls and
1.2M journal Next calls, zero errors,500k invocation bytes/84.7M journal bytes,
one iterator and one cursor per stream, and at most64 recent trace calls.
Plain and traced public reports must both complete within20s. Named PASS44.02s.
Traced being faster in one sequential pair is NOT evidence of a speedup or a
stable overhead estimate; it qualifies the original deadline with tracing on
this cohort. It remains a small-payload cohort, separate from refill-sized,
semantic-corruption and real consumer-leader-loss controls.

## Independent review and preservation

647/648 selected Go/module inputs match their executed Git revisions, retained
source bytes and unchanged before/after ledgers. Actual live SDK executable and
file hashes match; full build info explicitly records clean executed VCS.
All five captured container server copies/build fields/inspect mounts verify.
Copies come from /nats-server, not server-process /proc descriptors. Not an
exhaustive compiler/toolchain/external-module input inventory.

The first pre-open640 clone files match the previous independently verified
published interrupted-cohort archive member/digest. The second pre-open ledger
matches the first campaign's closed post-run ledger. Copied stores continue under
the original logical NATS identities with fresh Docker resources. Clone bytes
stay at /tmp/js-wf-r5-retained-profile-restored-20261005/copied-stores and are not
duplicated by these proofs; original input corpus bytes remain in the earlier
published archive. Post-run ledgers and all producer/source/process/profile/
comparison artifacts are retained. Both complete proof archives and every
member/split part are read back/hash-verified:683 members/99,395,705B and681
members/105,934,011B. Inspect qualification.json and per-campaign manifests.
The reviewer initially used wrong JSON key case and stopped before qualification;
its rejected source/error record is retained, then actual schema checks pass.
The trace test's initial API-name compilation error was corrected in6176205;
no failed compile is promoted to native qualification.

## Concurrent million-timer observation

The existing candidate campaign stays live. First planned three-server SIGKILL
occurs07:31:51.311518302 UTC, healed07:32:08.403717152:17.092s after333,338
receipts. Deliveries resume; maximum observed lateness17.681s remains below30s.
7 redeliveries/7 ack errors/53 fetch errors remain recorded, not discarded.
This observation confirms a running SDK/unit handle, not final campaign pass,
physical drain, candidate adoption or current-main scheduler qualification.
Running final_stream_messages/final_ack_pending zeroes remain placeholders.
Both R5 performance campaigns finish before this restart; VM load is shared.

## Scope and next work

This accepts the focused executed-source full-cohort reader/interruption and
trace deadline gates. Historical checkpoint1750/other failures remain unconfirmed.
Actual24h, full/final-source matrices, legacy/domain and original million physical
drain gates remain open. An unchanged Tier1 state-machine corpus is not rerun.

Disk headroom is limited. Derived Go build cache was cleared only after no
visible descriptors/build processes remained; actual SDK/server binaries,
original stores/source/receipts/proofs remain. Cache rebuild cost is the tradeoff.
Do not launch a growing24h fixture until space for its retained stores is secured.
The existing million campaign remains the same live handle.
