# Production canonical source matching — 2026-10-11

GraphStart.MatchesInvocation now admits the complete value list and exact spelling
of every owned source field. Required token/input hash/parent fields have one
matching value. Optional parent fields and Wf-Input-Ref must be absent when
unused. Duplicates, conflicting second values, aliases and empty lists fail
closed; empty-list reads no longer panic. Unrelated transport headers are allowed.
Existing exact pointer bytes, subject and nonzero sequence checks remain. Bound
callers still compare the sequence against their durable invocation. This uses
production declarations; the raw auditor keeps its own declarations and checker.

The baseline regression at367d65d plus the new test exits1 because a duplicate
start token incorrectly matches (source match=true want=false). baseline.log and
baseline-proof.json retain that failure; the baseline source hash receipt was
prepared after the command. No native test or qualification failure was retried.

## Verification

```sh
go test -race ./journal ./client -run '^(TestGraphStartMatchesInvocationHeaders|TestGraphCanonicalStart|TestCanonicalStart|TestCanonicalBoundStart)' -count=1 -v -timeout=3m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-canonical-start-headers-native-20261011 go test -race ./worker -run '^(TestNativeCanonicalStartRecoveryWorkerReplayAndPurge|TestNativeGraphContinuationChildPromise)$' -count=1 -v -timeout=6m

python3 docs/scale/canonical-start-source-headers-2026-10-11/guard.py
```

The production race command exits0: journal1.125s/client1.206s. All64 header
controls pass (4 positive/60 negative) with two journal start and six client
recovery/repair regressions. Native race exits0 in116.209s: eight R1/R3 start modes
(ordinary, reserved, source_committed, bound_no_enqueue) recover, fence duplicate
worker effects, replay, purge with protected input and reuse IDs with advancing
high water. Each replica reports zero physical chunks after drain. Two native
R1/R3 child cases provide complete valid parent headers and raw-audit receipts
with four checked parent frames and two retired-child projections.

462 production/relevant test/mod/sum inputs were hashed before the commands and
verified unchanged afterward. The actual native worker executable was captured;
the short journal/client executable captures were missed. The473 closed raw SDK
server files (21,449,994 bytes) retain hashes/modes/nanosecond mtimes. Native start
recovery fixtures use temporary stores removed after the test; their logs are
retained, not a complete restorable media proof. guard.py requires every focused
control, regression, native start mode, raw receipt and physical-drain receipt.

The existing normal/race graph-journal CI selectors now include the new matcher
test. Native start/child tests already run in their existing jobs. No full hosted
pipeline execution is claimed. The independent auditor's232 physical header
controls and its prior native compatibility evidence remain in the preceding
raw-start-source-headers evidence directory.

## Remaining scope

Unrelated header interpretation, legacy-mode header admission, complete parent
causal/source/lease history, scalar presence/null, orphan/retired ownership,
rejected-prefix reconstruction and original broad simulation/scale/fault/soak/
rollout gates remain open. This production change is outside all three frozen
live campaigns; latest-main full qualification is not inferred. Public
continuation admission/import/online collection remain disabled.
