# R5 retained audit phase, cursor and delivery-window diagnostic

A verified copy of the previously failed100k cohort's five native file stores
is reopened with its original NATS cluster/server names and fresh Docker
container/network/route names. Source streams retain five replicas throughout.
Initial store copies match all720 corresponding files in the published failed
archive; all720 original files still match after the diagnostic. Originals are
untouched. Full-report comparisons concern the same100,000 invocations and
1,200,000 journal entries, not four different cohorts.

## Observed comparisons

Every attempt uses the original20-second audit context, normal executable,
GOMAXPROCS2/GOMEMLIMIT2GiB, explicit routes, snapshot state and streaming journal.
Per-record timing and CPU profiling add diagnostic overhead.

| Cursor replicas / window | Elapsed | Journal records visited | Full report |
| --- | --- | --- | --- |
| 5 /512 | 20.011558262 s | 1,154,765 | Deadline exceeded |
| 1 /512 | 16.813048288 s | 1,200,000 | Exact100k/1.2M report |
| 5 /4096 | 15.738929818 s | 1,200,000 | Exact100k/1.2M report |
| 5 /512 recheck | 20.035101198 s | 1,052,481 | Deadline exceeded |

Each actual cursor configuration is independently read back. For the4096 case,
the journal scan takes13.597 seconds, with3.298 seconds in the checker visitor;
most measured scan time is outside the visitor. The CPU profile collects8.13
CPU-seconds over15.74 wall-seconds, with client parsing, metadata decoding and
allocation/GC represented in the top samples. This does not isolate a sole
server/network/CPU cause or establish a robust speedup from single trials.
The recheck exposes the continued small-window deadline failure after warming.

The named diagnostic PASS means all comparisons ran and recorded their own
verdicts. It does not turn either failed attempt into a pass or qualify a release,
natural interruption, worst-case payload buffering, complete matrix or24h soak.
Production reader defaults remain512 records and stream-matching replicas.
The known fixture's small payloads do not prove safe buffering for4096 arbitrary
records. Next is a byte-bounded pull candidate, then full interruption/negative
controls before adoption or another soak. Original deadlines remain unchanged.

## Startup attempts and recovery scope

Attempt1 uses fresh NATS identities and cannot elect a metadata leader before
its original4-minute readiness budget; failed248.81-second originals retained.
A restored-identity fixture now preserves NATS names independently of fresh
Docker names and requires every copied node store. Its validation/default-name/
route/store controls pass under race; route censuses use logical server names.
The focused R5 interruption fixture is Linux-only, matching its Docker dependency.

Attempt2 restores original identities and monitoring reports100k/1.2M records
on every physical replica, but collector readiness exhausts its global context after247.91 seconds.
It made both Stream and subsequent Info requests without individual request
limits; the exact failing request/path remains unconfirmed. Attempt3 bounds individual readiness requests to2 seconds,
uses the freshly returned cached Info and records readiness failures. It continues
the verified restored clone and completes all comparisons after87.15 seconds.
Individual API probes through all five nodes return retained stream leader
identities during this attempt. No prior startup failure is erased.

All three actual live test executables/build fields, inspected live container
executable copies, unchanged captured source inventories/overlays, import and
continuation ledgers, failed/successful terminal outputs, all four CPU profiles,
phase reports, API probes, node logs and producers are preserved in the verified
2018-member archive. See archive-verification.json for hashes and12 parts.
The three primary roots and mutable copied stores remain on the VM; copied-store
post-state hashes are archived, but those derived stores are not duplicated in
this archive. The immutable initial store bytes remain in the earlier published
failed archive. Compiler dependencies are not exhaustively captured; live server
copies are from inspected paths, not `/proc/exe`. Test executables use `/proc/exe`.
Assembly copies were removed only after full archive/part/hash verification;
primary source/executable/store roots remain intact. This is documented separately.

The native million/24h candidate continued delivering concurrently; its source,
processes and stores were not changed. This overlap is part of the timing scope.

The retained API-probe source is the final five-node version. Earlier one-node
probe source revisions are not separately retained; their outputs are diagnostic
observations and do not establish source-complete standalone qualifications.
