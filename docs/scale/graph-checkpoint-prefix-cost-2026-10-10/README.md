# Checkpoint publication work and phase diagnostics

Development evidence over the production GraphStore and in-memory graph publication transport. This directed test uses scheduler seed 23 and deterministic entry-read budget injection; it is not an additional seeded family, a full inventory campaign, native latency evidence, or actual 100000-entry acceptance.

`GOMAXPROCS=2 GOMEMLIMIT=1536MiB go test -p=1 -race ./journal -run '^TestGraphCheckpointPublicationPrefixCostAndReadBudget$' -count=1 -v` passed in 36.858 seconds.

| Entries | Initial publication entry reads / total Gets | Indexed retry entry reads / total Gets | Compaction entry reads / total Gets |
| --- | --- | --- | --- |
| 36 | 37 / 111 | 4 / 17 | 40 / 1599 |
| 260 | 261 / 783 | 4 / 17 | 264 / 17291 |
| 1028 | 1029 / 3087 | 4 / 17 | 1032 / 83091 |

The injected 16-entry-read budget rejects initial publication with context.DeadlineExceeded. The canonical checkpoint remains absent, count and tail remain unchanged, and its reader pin is released. Retrying without the budget publishes the same frame. Indexed retry stays bounded and subsequent compaction succeeds. Counts include the graph transport's internal reads; they are not server RPC counts or isolated CPU timings.

Worker operation observations now distinguish canonical checkpoint publication/confirmation, legacy snapshot write/purge, and archive compaction inside the existing overall continuation publication timing. The limit fixture records these phases and attempts a bounded independent final status/root inspection on failure. The production publication deadline, cap, collector and public admission remain unchanged. The old actual-cap failure still has no exact phase receipt; do not infer one retroactively.

The first budget64 native development attempt failed after second padding with an unknown append outcome. Its reconstructed final cursor is suspended at count/tail59, checkpoint present and zero reader pins. The fixture still passed nil delivery operations in this attempt, so no phase timing was emitted. That wiring is corrected. This failure is preserved, not accepted or attributed to a server defect. It ran alongside other qualifications.

Corrected budget20 R1/domain/archive development run passes under race in 22.576s: both checkpoints, all20 ordered records, terminal slot19, zero forbidden effects. The two measured checkpoint publications take376.7/685.9ms; archive phases2.906/3.793s; whole publication3.669/4.779s. These are observed diagnostic timings, not performance gates. All three operation labels emit twice. Selected canonical indexed/unpublished handoff and legacy global-limit controls pass under race in57.444s. These tests run against the working development source; they are not frozen source qualification.
