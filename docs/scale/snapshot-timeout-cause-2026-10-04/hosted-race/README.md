# Corrected snapshot-reader source: complete hosted Tier1 race qualification

[Run 37172701931](https://github.com/AntPAllen/js-wf/actions/runs/37172701931), job 111348770188, completes successfully at exact `283ba325c31fe67194f0a0d7e11665b0bea405ed`. Artifact 11293000901 retains the actual race executable and full suite evidence.

Independent review verifies 175 passing top-level tests, the two documented trace-only skips, all 391 pinned regressions and all 121 scalable workloads at contiguous seeds 1–1,000: **121,000 workload bodies**. Aggregate counters including fixed/repeated tests: 124,847 schedules, 1,807,121 choices and 27,606,589 transport events. Package/wall time: **2,183.371/2,183.75 s**.

All **1,160** selected captured source inputs match before/after and exact executed Git. Actual executable SHA256 is `86c87adc0a3955ca45724300bd25b4d6400c7f5ba0873a2502b88570e779d023`; downloaded execute permission was restored for inspection without changing bytes. Actual build info proves race instrumentation, the compiled test list matches the retained list, exact-source AST regeneration matches the seeded inventory, and complete raw events regenerate the producer result byte-for-byte. Run/job/artifact identities and terminal success bind the executed source.

A separate 685-input equivalence ledger compares all tracked non-test Go/module inputs, complete simulator source/pins and Tier1 race producer/verifier with observed main `59bc2c3`; every selected byte is identical. Later unrelated integration tests, documentation and other reviewer/workflow edits are excluded from that equivalence claim.

`proof.tar.gz` retains all original artifact files including the actual executable, complete API metadata/logs, independent review, runtime-equivalence ledger, exact executed reviewer and preservation script. All 25 members are reopened and SHA256 verified before publication; `manifest.json` records each hash and complete archive digest.

This requalifies the full race1k suite after the snapshot error-cause correction. Full normal100k run 37172700747 is still in progress; real-cluster matrices, original million-timer physical drain and actual full-matrix 24-hour soak remain separate open gates. This does not establish the original server cause or qualify the failed continuation/promise restart case.
