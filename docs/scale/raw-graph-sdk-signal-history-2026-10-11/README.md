# Independent SDK checkpoint signal history — 2026-10-11

The checker distinguishes worker delivery (`SignalConsumed`) from SDK use
(a matched step completion). It reconstructs use for ordinary signal waits,
synchronous child calls, timer/signal selections and Select cases. A selected
signal must have arrived earlier, match the requested name, be unused and be
the oldest unused arrival of that name. Per-name queues keep this check constant
time per selection. Cached promise and timer selections do not consume another
signal; the separate promise history audit verifies cached selection provenance.

The latest worker-annotated SDK checkpoint must have exactly the reconstructed
consumed set, every remaining pending signal, the original names/payload bytes,
and the exact journal-prefix signal cursor. This covers ordinary buffered user
signals as well as child signals. Pending owned payloads are bound by their
original verified hash; inline payloads are compared directly. Complete physical
reference and canonical queue/input checks still apply independently.
No SDK replay, production selection or worker restoration is called.

## Actual completed evidence

```sh
go test -race ./integrity -run '^(TestRawGraphCheckpointSDKSignalHistory|TestRawGraphCheckpointPromiseHistory|TestRawGraphCheckpointSDKStateHistory|TestRawGraphCheckpointMetadataBindings)$' -count=1 -v -timeout=3m
go test -race ./integrity -run '^TestRawGraphCheckpointSDKSignalHistory$' -count=1 -v -timeout=3m
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise|TestNativeGraphContinuationBufferedSignals)$' -count=1 -v -timeout=8m
```

Actual exits0: combined integrity1.305s, final signal1.025s, native SDK120.003s.
Eight component positives cover ordinary/call/timer/select use, timer branches
cached promise reuse composed with the promise provenance audit, and selection
of a later sequence while an earlier signal of another name remains pending. Twenty-three
controls reject changed cursors, omitted/fabricated consumed sets, missing/extra/
used/misnamed/changed/duplicate pending signals, unknown/reused/out-of-order
selections and invalid timer/Select branches. These are decoded-history component
controls, not complete physical-reference-valid graph mutations.

The initial combined command also passed the state, promise and metadata fixtures.
After it completed, a redundant signal positive was removed and the cached-promise
case was strengthened to compose the two independent history checks; the final
component command verifies that source. A subsequent positive checks per-name
ordering, with the final command and source hash retained in name-order-race.log
and review.json; the prior seven-case signal-race.log remains preserved.

Eight actual R1/R3Domain SDK normal, buffered-user-signal, child-promise and
buffered-child cases pass the extended raw audit. Four have retired child
projections reported separately. The executable receipt retains actualPID216265,
SHA256 and race build flag; all339 captured production/module/worker-test source
hashes were rechecked unchanged. This is focused development evidence, not a
clean frozen latest-source complete qualification.

CI now requires all four native families and eight receipts, four retired-child
and four ordinary-parent cases. Its complete guard and the new signal-history
subset of the raw graph guard were executed against the actual logs. The exact
assertions are retained here. Unrelated raw graph cases are not inferred.

## Remaining scope

Select case readiness/clock ordering, cancelled timer history, ambiguous promise
cache completeness, original child-source history, every historical checkpoint,
protected reader application replay, complete envelope/cursor schema, orphan
projections and original scale/fault/soak/retention/rollout gates remain open.
Unannotated journal-only frames receive structural checks; supplied signal state
without SDK operations is not claimed as reconstructed history. Public admission,
import and collection remain unchanged. Both frozen campaigns retain their earlier
sources and do not qualify this later auditor change.
