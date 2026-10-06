# Current-source Tier2 partition campaign — 2026-10-06

Prepared full seeds1–200, each original ten-minute duration, for the partition row. Existing per-test18m SDK timeout and job25m timeout, intermediate20s/60s audit limits, original workload/fault count, history/integrity/latency/drain gates remain.

Original full-matrix37149506857 atc4fed retains seventeen failed partition shards. Completed job111287268311 shows seed1 stopping at batch50/cutoff1400 on terminal state missing through weak KV Get. Its complete raw job log is retained here; source comparison confirms the old lookup and current default administrative leader lookup. Historical failure remains a failure, with native server-side cause unconfirmed. This is an identified reader-path change, not a proof that every failed seed has the same cause.

Current handler loss and leader-read changes have focused native/seeded evidence. This campaign tests their combined behavior against the full original partition range. No old failure or other row is superseded. Full13-row/current-source and24h gates remain separate. Dispatched run[37486948256](https://github.com/AntPAllen/js-wf/actions/runs/37486948256) atfcf2e94 is queued; terminal/native acceptance is pending. Local24 matrix parser and one workload-source tests pass; the planner covers exactly200 singleton seeds1–200. A read-only user-unit observer pins the run/source, records job states and collects complete available terminal logs/raw artifacts/source selections into a verified archive without qualifying them. Observation expiry never means native terminal or authorizes restart. [Launch evidence](launch/).


## Independent raw partition review

`scripts/check-tier2-journal-shard.py --row partition` now verifies exact minority routes[4,4,0], positive acknowledged majority sequence, nineteen30s fault schedules and the original35s whole-cut fault budget. Source/job/artifact binding rejects wrong row, range, duration or source. Use `--job-layout single --first N --last N` for the new `leader (N)` singleton jobs; the default `shard` layout requires `leader (partition, RANGE)`.

Every raw latency sample is recomputed from its timestamps, with exact counts/per-type p99/progress/deadline comparison. All three independent history models are compiled only after their dependencies match the recorded SDK source. Recorded-source campaign and Go event-duration helpers remain mandatory. Independent strict RFC3339Nano parsing removes an unrelated R5 helper dependency; the historical positive job predates that helper and initially stopped at its absence, with no native rerun.

Fresh actual provider artifact11153871883/job110315797208/run36845868029 at82a7d6c passes complete review:1568 invocations/17307 journal entries/19 faults/7392 latency samples/2016 history operations. Every ZIP member and provider digest match the fresh extracted corpus. Three independently mutated disposable copies reject absent majority write, one-nanosecond latency inconsistency and a second successful Start, while the original bytes remain unchanged. Eight parser/binding/control tests also pass.

[Reviewer controls](reviewer-controls/) preserve the complete historical positive/control corpus, model inputs and actual model executable. This validates the reviewer and qualifies that historical selected seed only. It does not qualify the new200 campaign, current runtime, parent full matrix, physical stores or24h; native SDK/stores are unavailable for this old workflow. No broker was reopened.
