# Fresh complete Tier1 gates after the snapshot diagnostic correction

Both dispatched runs bind exact production source
**283ba325c31fe67194f0a0d7e11665b0bea405ed**:

- [Normal100k run 37172700747](https://github.com/AntPAllen/js-wf/actions/runs/37172700747)
  requests 100,000 seeds per workload through the complete suite producer.
- [Race1k run 37172701931](https://github.com/AntPAllen/js-wf/actions/runs/37172701931)
  requests the complete default race suite.

Exact dispatch commands, executed workflows and initial API run/job metadata
are preserved with hashes. These are new complete gates for the changed
production fingerprint, not a subset intended to replace the release corpus.
Initial queue/dispatch metadata establishes no executed seeds, pass or release
qualification. The earlier complete 9ecc37c gates remain historical evidence.

The first-lookup error-cause fix preserves snapshot read bounds and failure
classification; the original combined continuation/promise after-manifest CI
case remains failed and its server cause unconfirmed. Existing Tier2/Tier3
campaigns retain their exact executed-source scope. When independently rerunning
history models for those older campaigns, compile from their source-identical
inputs rather than the now-modified main worktree; strict dependency hash
checks must not be bypassed. Full matrices, the original million-timer physical
drain and required actual 24-hour full-matrix soak remain separate requirements.
