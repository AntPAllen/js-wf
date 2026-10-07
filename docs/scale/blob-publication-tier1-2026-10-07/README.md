# Blob publication: common Tier 1 focused qualification

The experimental production decision functions now run through `BlobPublicationTransport` and the common `Scheduler`, `Trace`, replay dispatcher, failure-trace path and pinned corpus. The source graph contains **124 seeded workloads and 410 pins**; this report qualifies the newly added workload at **1c4e4f404e5541b436ab45fe942d67d00a4bb3c3**, not the full graph.

Fifteen modes cover paused publication/upload, partial preparation, lost pin/ready/upload/commit/fence/close/delete replies, dropped publication, shared references, preservation of an existing root, acquire versus close, and an unsafe head-reset negative control. Each schedule also chooses the order of five enabled actors: two competing writers to one root, another shared-reference writer, collection and retirement. Committed references are checked after each actor and all terminal objects must be reclaimed. Every generated trace is replayed exactly, including transport decisions and data digests.

| Profile | Completed bodies | Choices | SDK duration | Pins |
| --- | ---: | ---: | ---: | ---: |
| Normal | 100,000 | 600,000 | 42.47 s | 15 |
| Race | 1,000 | 6,000 | 5.79 s | 15 |

Both use one retained native Go test binary, count1, original 3m timeout, two Go CPUs and 512MiB. Actual live process identities are admitted across executable hashing; full build information binds the clean Git revision and race setting. All **2,325** selected committed source inputs match retained copies before/after. Offline review accepts both original native logs and rejects **24 actual-positive log substitutions**. Dedicated CI runs the race workload, pin dispatcher and standalone protocol controls; hosted acceptance is not claimed.

The normal SDK passed on its first execution. The initial producer's final-line format check incorrectly rejected the appended coverage summary. Its code and failure are preserved; a continuation corrected the review, ran the previously unexecuted race profile and archived both originals. The normal body was not rerun.

This closes focused common Tier 1 normal100k/race1k and saved-trace integration. It does not qualify the complete current124 graph, implement native transport adapters, enable production online GC, resolve physical chunk reclamation or qualify real-cluster/full release gates. The older full123 normal campaign is independently reviewed at its original source.

Full fixture archive metadata/inventory and S3 readback receipt preserve the actual binaries and inputs. [Independent review](independent-review.json).
