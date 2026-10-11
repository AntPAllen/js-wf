# Independent SDK checkpoint state history — 2026-10-11

The raw graph journal checker now reconstructs the SDK state map from completed
`state_set` and `state_get` operations across the complete archived/live prefix.
Successful writes must return a found value whose bytes hash to the declared
request input. Reads must return exactly the presence and bytes established by
preceding writes. Failed operations leave the map unchanged. External completion
results require the canonical reference, exact owned edge and bounded actual
bytes; unknown result fields and non-object results are rejected independently.

For the latest worker-annotated SDK checkpoint, the frame's state key census and
each value must equal this reconstructed prefix map. Worker annotation identifies
SDK materialization; an unannotated journal-only frame receives structural state
validation, since such writers can supply state without SDK operations. This
does not call wf.Context, Replay, worker restoration or production frame admission.
It shares entry serialization and the unambiguous JSON wire decoder.

## Actual completed evidence

```sh
go test -race ./integrity -run '^TestRawGraphCheckpointSDKStateHistory$' -count=1 -v -timeout=3m
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=8m
```

Actual exits0: expanded history1.147s, native SDK83.036s. Ten positive raw cases
cover JSON/protobuf-v1 and archived inline writes, owned external writes, absent
reads, failed writes and present JSON null values. Eleven corruption controls
first pass the physical reference audit and then fail semantic checking:
changed/missing/extra frame state, mismatched write hash, missing write value,
failed write falsely materialized, fabricated read, null/unknown result shape,
and unowned/wrong external result bytes.

Six actual R1/R3Domain SDK normal, child-promise and buffered-child cases pass the
extended raw audit. These execute state writes and reads through real production
handlers and multiple continuation stages. Four explicitly distinguish the
retired child's projection from the fully audited parent journal. The retained
executable receipt captures actualPID214031, SHA256 and race build flag. All337
captured production/module/worker-test source hashes were rechecked unchanged.
The two additional positive integrity fixture modes were added during that run
and verified separately; their final source hashes are recorded in review.json.

Existing checkpoint pointer, materialized-state shape, journal and worker-metadata
fixtures passed a separate race command in1.303s after the fixture was generalized
to include actual SDK state operation pairs and a three-entry archived prefix.
The initial six-positive state command also passed in1.162s; both logs remain.
The native SDK CI guard and new state-history guard subset were executed against
actual logs. The exact Python assertions are retained here. Unrelated raw graph
cases and a frozen latest-main whole-suite qualification are not inferred.

## Remaining scope

Promise outcomes, consumed/pending signal sets and cancelled timer sets still
need full reconstruction from SDK history. The checker validates the current
checkpoint pointer, not every historical checkpoint completion. Protected reader
application replay, complete envelope/cursor schema, orphan projections,
source-frontier history, all-peer physical durability and original scale/fault/
soak/rollout/retention acceptance remain open. Public admission/import/collection
policies are unchanged. Both frozen campaigns retain their earlier sources.
