# Failed sustained rollout: wire and latency outlier review

This is analysis of preserved originals, with no native rerun and no writes to
original fixture files. Copied wire/admission/dispatch/latency/fault files bind by
SHA256 to the independently verified failed-run archive. The protobuf schema
and wire verifier match executed source `ab5cd71`.

Independent generated Python codec decodes all392 invocations /4345 worker
records:211protobuf and4134JSON, seven mixed-writer invocations. Format attribution
matches admitted worker generation/PID. This proves this wire check within a
**failed parent**; it does not qualify sustained/default/fullmatrix latency.
Actual SDK observer captured110 of124 worker session PIDs;14 were unobserved.

The56 short-workflow terminal samples contain one ≥30s; nearest-rankp99 is
33.910941280s for `tier3-1-batch-4-5`. Its four journal records are allJSON:
Started/StepRequested by worker0generation7 epoch3378; StepCompleted/Completed by
worker4generation11 epoch4048. It is not one of the mixed-encoding invocations.

| Observation (UTC) | Event |
| --- | --- |
| 09:57:23.225 | Invocation enabled |
| 09:57:23.530 | Worker0generation7 acquires delivery1 |
| 09:57:24.636 | That owner is reaped by SIGKILL |
| 09:57:36.525 | Delivery2 finds lease held, then NAK |
| 09:57:41.749 | Worker2generation8 acquires delivery3 |
| 09:57:44.340 | That owner is reaped by SIGKILL |
| 09:57:54.649 | Worker4generation11 acquires delivery4 |
| 09:57:57.136 | Terminal observed |

The timeline identifies repeated owner loss before completion. It does not prove
complete latency causality or a format defect. Model this concrete sequence
before another sustained run. Original strict30s gate remains failed; the prior
single-kill TTL configuration mismatch does not erase this repeated-kill case.

Read/hash-verified analysis archive: 653 members / 380,993 bytes / 1 part, SHA256 `2242bd87286c7dfe310dc63ed592b4a5d464f192c318c230f3b53ed706fda0db`. Full failed native stores/source remain in [failed originals](../10m-failed/). See [independent review](independent-review.json) and [archive verification](archive-verification.json).
