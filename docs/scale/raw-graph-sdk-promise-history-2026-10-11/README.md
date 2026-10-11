# Independent SDK checkpoint promise history — 2026-10-11

The raw journal checker now binds each saved promise outcome to a preceding
selected child signal, its owned original body, the parent child declaration,
child generation and original result reference/hash. An external child result
must also have an owned edge in that original SignalConsumed record. The
existing checkpoint audit separately verifies current checkpoint ownership
of the materialized external result. Saved outcome bytes must equal the selected
body after JSON whitespace compaction, matching SDK RawMessage serialization.
No production SDK replay or child/frame admission validator is called.

Explicit `select_many` promise selections require a saved outcome. A zero signal
sequence must reuse an existing candidate; a proven cached promise cannot later
consume another signal as a fresh promise selection. Later ordinary signal
awaits cannot replace the explicitly established cache provenance.

## Journal observability limit

`AwaitPromise` and ordinary `AwaitSignal` both record request kind `signal`.
Therefore that record alone cannot prove whether a promise cache entry must
exist. The checker validates every saved outcome against possible selected child
outcomes, but does not require a cache entry for these ambiguous requests.
Multiple candidates remain possible; it does not reject a valid ordinary await
merely because it selected another child-named signal. An explicit promise
selection narrows the candidates and requires the entry.

Complete cache census for ambiguous awaits remains an implementation-plan
requirement. Proving it needs workflow-level replay evidence or a durable
distinction between promise and ordinary signal awaits. No journal schema or
runtime serialization is changed by this audit, and this limitation is not
treated as full I4 acceptance.

## Actual completed evidence

```sh
go test -race ./integrity -run '^TestRawGraphCheckpointPromiseHistory$' -count=1 -v -timeout=3m
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=8m
```

Actual exits0: expanded component race1.028s and native SDK85.974s. Nine positive
component cases cover inline/external outcomes, ordinary/explicit selection,
cached selection reuse, whitespace compaction, valid omission for an ambiguous
await and both earlier/later possible candidates. Fourteen negative controls
cover changed outcomes/name/declaration/generation, missing original ownership,
wrong/missing body bytes, unowned original external results, omitted explicit
caches, malformed selection, reuse without history, fresh consumption after
explicit caching and selection before arrival. These are decoded-history
component controls, not complete physical-reference-valid graph mutations.

Six actual SDK R1/R3Domain normal/child/buffered-child cases pass the extended
raw audit after real continuation execution. Four have retired child projections
reported separately. Native executablePID214951, SHA256 and race build flag are
retained; all338 captured production/module/worker-test source hashes were
rechecked unchanged. The final additional ambiguity case was verified separately
and its test hash is in review.json. Existing raw state-history and metadata
fixture regressions passed race in1.197s. The initial component command passed
in1.034s; all logs remain.

CI selects the component test and requires every positive/control. Its new guard
subset and the complete native SDK guard were executed against actual logs;
the exact assertions are retained. This is focused development evidence, not
frozen latest-main whole-suite qualification or execution of unrelated CI cases.

## Remaining scope

Ambiguous promise cache completeness, complete SDK signal/timer history,
original child-source history, every historical checkpoint, protected reader
application replay, complete cursor/envelope schema, orphan projections and
original scale/fault/soak/retention/rollout acceptance remain open. Public
admission/import/collection policies remain unchanged. The two frozen campaigns
retain their earlier sources and do not qualify this later checker.
