# Opt-in provisioning API diagnostics

`WF_TIER3_UPGRADE_API_TRACE=1` enables a dedicated JetStream client on the rolling fixture's existing connection, with the same default API options. Only its fallback provisioning call is traced; fleet operations remain on their existing client. `backend_check.api` captures request/response direction, exact subject, UTC and monotonic elapsed time, copied payload bytes (JSON base64) and response headers. Callbacks copy into memory; the proof is written after the call. Transport errors have no response callback, so the final unanswered request remains visible alongside the existing primary error and operation deadline.

The hosted workflow exposes boolean `upgrade_api_trace` (default false), limited to the rolling-upgrade row. The 60-second proof deadline, provisioning semantics, startup readiness, recovery gates and production/simulation/Tier1 code remain unchanged. Default proofs omit the new API field.

`TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest` runs a real NATS request/reply connection with one metadata response then a deliberately unanswered second lookup inside production `EnsureAuto`. It verifies exactly request/response/request, retained response bytes/subjects/timing and preserved `context.DeadlineExceeded` cause. Focused race test passes in1.252s; the existing five matrix planner guards also pass. This is diagnostic instrumentation validation, not native rolling-upgrade qualification or a cause/fix claim for the historical failure.

```sh
GOCACHE=/tmp/js-wf-go-build-cache-20261004 GOTMPDIR=/tmp/js-wf-tier2-model-review-20261004/gotmp GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./integration   -run '^TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest$'   -count=1 -timeout=1m
```

A focused hosted seed15 ten-minute diagnostic must still execute at the pushed prepared source with `upgrade_api_trace=true`, the original SIGKILL profile and Start-gap cut. Preserve its actual API proof, original stores and source attribution before interpreting the outcome. No old parent repair or full-matrix/24-hour qualification follows from a focused success.
