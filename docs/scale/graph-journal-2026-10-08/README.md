# Generation-aware graph journal component regression

Frozen source: `d0bd14e1ec67df199c0a10dfe7eecdd4dfcf2ba9`. All 1,388 selected tracked Go/workflow/module/simulation inputs matched Git before execution and remained unchanged afterward.

| Run | Coverage | Package elapsed |
| --- | --- | --- |
| Normal journal | All five new graph journal groups, native R1/R3 65-entry/large payload/shared input/adapter reopening/drain | 6.246 s |
| Normal journal simulation | 100,000 generated schedules and exact replay, nine modes, all 64 graph pins | 381.333 s |
| Race journal | Same five focused graph journal groups, native R1/R3 | 48.903 s |
| Race journal simulation | 1,000 generated schedules and exact replay, nine modes, all 64 graph pins | 55.253 s |

All commands use count1/two Go CPUs/512MiB. The 100,000-schedule journal run uses an original 10-minute envelope selected before launch; all other commands retain five minutes. No retry, restart or deadline extension. Complete event logs, exact commands/environments, source hashes and review code are retained. The review checks the exact new journal test/pin inventories, native replicas, mode counts and no failed/skipped bodies.

The pre-commit native development run initially failed a drain assertion that compared total retained subjects to zero. Graph authority intentionally retains metadata/tombstones. The assertion was corrected to inspect actual chunk subjects; no storage implementation or acceptance target was changed. The frozen component runs above use that corrected assertion.

Scope is the explicit-generation graph journal adapter and its ownership/lifecycle decisions. Native controls reopen adapters against running stores; they do not restart peers or simulate abrupt process/power loss. This is standard local regression, without independently admitted SDK executable/peer-media provenance. It is not full current-source Tier1, full existing journal package, native release/matrix/24h/million-drain, deployed schema rollout or canonical runtime migration qualification.

Existing worker/default Store, SDK input/results/state/terminal, signals/reconcilers, snapshots/continuations, importer and retention still require integration. Callers must verify the invocation sequence against canonical start state, and explicitly graph-own every external payload. Production GC remains quiescent.
