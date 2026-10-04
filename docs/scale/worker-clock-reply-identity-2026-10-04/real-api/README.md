# Strict clock reply observer: real API conformance

`TestMatrixWorkerClockReplyIdentityRealCluster` boots three actual in-process NATS nodes with separate file stores, creates a three-replica `MATRIX_CLOCK` stream and publishes/reads two probes through each pinned peer. The unchanged two-second probe deadline applies. All six accepted samples retain the correct worker and advance sequence, pass the existing freshness/unshifted-offset gate, and correspond to exactly six stored stream messages with final sequence six.

Normal run passes **2.169 s**; race run **3.239 s** (named test 2.16/2.20 s). Both logs retain the six actual worker/broker timestamps and sequences. This proves healthy actual API replies satisfy the new receipt/message identity checks. The prior deterministic mismatches and compiled original rejection control remain separate evidence.

Five observed fixture/cluster/module inputs match before/after execution. All 685 runtime/simulator/Tier1 producer bytes remain identical to race-qualified `283ba32`; existing normal100k/cluster campaigns retain their source scope. Test executables and temporary broker stores are not retained or independently reopened. This is a healthy three-node API contract, not the five-container clock fault row, full matrix, original timeout reproduction or 24-hour qualification.

All 12 proof members reopen and SHA256-verify before publication. `proof.tar.gz` includes logs, actual executed source, before/after ledgers, runtime equivalence, parsed samples and exact preservation script; `manifest.json` records hashes. `analysis.json` preserves metrics and scope.
