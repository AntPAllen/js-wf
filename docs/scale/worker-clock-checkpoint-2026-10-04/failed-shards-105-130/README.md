# Retained-state audit failures in clock shards 105–117 and 118–130

[Run 37164231641](https://github.com/AntPAllen/js-wf/actions/runs/37164231641) executes exact `79915ca41a5c5a23b9997eea5f3f66d82530ee30`. Both jobs terminate in failure. Their complete raw artifacts and terminal API metadata/logs are preserved and independently bound to the executed source.

| Job / shard | Failed seed | Last audit | Duration | Partial invocations / journals / entries / terminals |
| --- | ---: | --- | ---: | --- |
| 111324439576 / 105–117 | 112 | batch 90, cutoff 2,520 | 60.002074564 s | 2,520 / 1,921 / 15,351 / 1,920 |
| 111324439574 / 118–130 | 121 | batch 90, cutoff 2,520 | 60.000480289 s | 2,520 / 500 / 6,538 / 499 |

Each retained-state audit fails with `terminal state missing: context deadline exceeded` before in-flight result calls return `context canceled`. Source inspection shows that this wording wraps the terminal-value lookup error after a terminal journal entry has been read. A lookup deadline is not proof that the terminal key is absent or corrupt. The server cause is unconfirmed. All five worker subprocess logs in each failed seed contain `PASS`; these failures differ from the earlier periodic worker clock-proof exits.

For seed 112, the affected `matrixshort/tier3-112-batch-56-5` client history records a successful completed result at `05:17:06.096772791Z`. The failed audit begins at `05:20:29.131314442Z`, **203.034541651 s later**. The preserved history operation and raw audit are included in `analysis.json`. Seed 121's affected child has no directly matching operation in the uploaded top-level client history; no corresponding child completion claim is made.

The preceding batch-80 audits pass in 18.712678888 s and 19.014620517 s respectively. The executed old source lacks per-attempt timing, so the 60-second aggregate cannot identify individual attempt durations or the first API failure. Current main already preserves checkpoint-attempt timings and primary failure before cancellation; this analysis does not attribute a timeout fix to that instrumentation or promote another revision's success.

All 759 selected source hashes match executed Git before/after every actually executed seed: 105–112 and 118–121. Seeds 105–111 and 118–120 have named producer passes only; they are not independently qualified here. Seeds 113–117 and 122–130 were not executed. Full parent, missing ranges, complete clock row/matrix and original 24-hour gates remain open. No timeout/cadence/gate is relaxed and no fresh trial is started.

Raw artifacts **11294386997** (ZIP 22,536,260 bytes) and **11295271287** (ZIP 10,806,888 bytes) are downloaded in full. Original-store artifacts **11295046238** (ZIP 853,938,728 bytes) and **11294219833** (ZIP 440,020,294 bytes) are references only: not downloaded, hashed or reopened. Actual workload executable bytes are not verified in this review. The complete failure bundle contains 1,160 members / 34,190,830 compressed bytes, including every raw input, source hashes, API/logs, analysis and exact executed review/preservation script. Every archive member is read back and SHA256-verified; all original inputs remain unchanged afterward. Verify the complete archive digest in `manifest.json` before extraction.
