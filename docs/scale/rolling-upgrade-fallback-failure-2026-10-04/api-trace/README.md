# Opt-in provisioning API diagnostics

`WF_TIER3_UPGRADE_API_TRACE=1` enables a dedicated JetStream client on the rolling fixture's existing connection, with the same default API options. Each pinned peer's native check and the fallback provisioning call are traced; fleet operations remain on their existing client. `nodes[].native_check.api` and `backend_check.api` capture request/response direction, exact subject, UTC and monotonic elapsed time, copied payload bytes (JSON base64) and response headers. Callbacks copy into memory; the proof is written after the call. Transport errors have no response callback, so the final unanswered request remains visible alongside the existing primary error and operation deadline.

The hosted workflow exposes boolean `upgrade_api_trace` (default false), limited to the rolling-upgrade row. The 60-second proof deadline, provisioning semantics, startup readiness, recovery gates and production/simulation/Tier1 code remain unchanged. Default proofs omit the new API field.

`TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest` runs a real NATS request/reply connection with one metadata response then a deliberately unanswered second lookup inside production `EnsureAuto`. It verifies exactly request/response/request, retained response bytes/subjects/timing and preserved `context.DeadlineExceeded` cause. Focused race test passes in1.252s; the existing five matrix planner guards also pass. This is diagnostic instrumentation validation, not native rolling-upgrade qualification or a cause/fix claim for the historical failure.

```sh
GOCACHE=/tmp/js-wf-go-build-cache-20261004 GOTMPDIR=/tmp/js-wf-tier2-model-review-20261004/gotmp GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./integration   -run '^TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest$'   -count=1 -timeout=1m
```

The focused hosted seed15 ten-minute diagnostic has completed with `upgrade_api_trace=true`, the original SIGKILL profile and Start-gap cut. Its native/API/original/history review is accepted below. No old parent repair or full-matrix/24-hour qualification follows from this focused success.

## Focused diagnostic dispatched

[Run37194088941](https://github.com/AntPAllen/js-wf/actions/runs/37194088941) executes source `520316e04bb3b7174fd013817d375f5f6135a44a` with the exact settings above. Source/API/dispatch observations are retained in `seed15-launch/`. The focused diagnostic is now independently accepted:1848 invocations,20355 journal entries, five upgrades and all three history models /2381 operations. All120 API requests received responses; fallback checks took22–35ms and did not reproduce the earlier timeout. Complete original/member hashes verify, but stores were not reopened and the older run did not retain its SDK executable. [Accepted proof and scope](seed15-accepted/). Historical parent and full-matrix/24h gates remain open.

## Native check trace coverage

Seed41 at79915ca fails during `peer-native-rejections`, before fallback begins. The opt-in trace therefore now also records each pinned peer's production `Ensure` call using a separate default-options client on that same pinned connection. Original operation context/deadline and semantic rejection rules are unchanged. Requests from a failed call remain in `nodes[].native_check.api`; default proofs omit the field. The original proof version4 remains compatible.

A second real NATS request/reply contract, `TestFiveUpgradeNativeTraceRetainsUnansweredRequest`, drives production native provisioning through one metadata response and an unanswered second read. It verifies retained request/response/request, payload/subject/monotonic timing and original deadline cause attached to the native check. Both trace contracts and existing native deadline/semantic controls pass under race in3.696s. This validates diagnostics and does not establish the historical timeout cause or qualify the failed parent.
