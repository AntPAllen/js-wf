# Current-source Tier2 partition campaign — 2026-10-06

Prepared full seeds1–200, each original ten-minute duration, for the partition row. Existing per-test18m SDK timeout and job25m timeout, intermediate20s/60s audit limits, original workload/fault count, history/integrity/latency/drain gates remain.

Original full-matrix37149506857 atc4fed retains seventeen failed partition shards. Completed job111287268311 shows seed1 stopping at batch50/cutoff1400 on terminal state missing through weak KV Get. Its complete raw job log is retained here; source comparison confirms the old lookup and current default administrative leader lookup. Historical failure remains a failure, with native server-side cause unconfirmed. This is an identified reader-path change, not a proof that every failed seed has the same cause.

Current handler loss and leader-read changes have focused native/seeded evidence. This campaign tests their combined behavior against the full original partition range. No old failure or other row is superseded. Full13-row/current-source and24h gates remain separate. Dispatch and terminal acceptance are pending.
