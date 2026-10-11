# Independent cursor capability/state admission — 2026-10-11

The raw auditor derives start/signal/checkpoint/archive capabilities from the
persisted cursor version(v1/v2/v4/v5/v6), independently of production configuration
and admission. The authority version must match. Legacy cursors cannot carry
canonical start/previous generation/signal/checkpoint/archive state. Canonical
cursors require input forest presence even after retirement. Only supported
forest names are admitted; input/archive counts and retained boundaries match
lifecycle state. Signals have signed-range, binding/source, consumed and repair
bounds, and pending invocations cannot carry signal state.

Cursor kind/count combinations and terminal-only purge are checked. Retired
cursors cannot retain publication tokens; canonical generation high water applies
to retired cursors too. Checkpoint pointer metadata is checked before the retired
branch, although retired frames/owned bytes still lack a full-history witness.
The three existing retired fixtures now clear publication tokens as real Retire
does. Projection-only counters remain explicit.

## Evidence

```sh
go test -race ./integrity -run '^(TestRawGraphCursorState|TestRawGraphCursorVersionCapabilities|TestRawGraphCursorEnvelope|TestRawGraphStartEnvelope|TestRawGraphRetiredProjectionEnvelope|TestRawGraphRetiredSourceProjectionScope|TestRawGraphStartSourceHeaders|TestRawGraphSignalReservationAndBinding|TestNativeRawGraphCheckpointAudit)$' -count=1 -v -timeout=4m

WF_GRAPH_SDK_RAW_AUDIT=1 WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-cursor-state-native-20261011 go test -race ./worker -run '^(TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise)$' -count=1 -v -timeout=5m
```

Actual race exit0 in20.628s:64 physical JSON/protobuf × legacy/archive × live/
retired cases (12 positive/52 negative) pass reference auditing before semantic
admission;35 metadata capability controls (10 positive/25 negative) span all five
versions. Capability controls alone are not full physical snapshots. Existing
cursor/start/source/signal/retired-projection controls and four native R1/R3
indexed/unindexed checkpoint cases pass. The extracted new CI subset requires
all99 receipts and was executed locally, not the hosted pipeline.

Native race exit0 in71.260s for four R1/R3 child/buffered-child flows. Each reports
two parent frames and one retired-child projection: eight historical frames,
four projection-only receipts.419 source inputs hashed before both commands remain
unchanged. Actual native executable capture is retained; the first race capture
was missed. All946 closed native files (43,409,675 bytes) retain hashes, modes and
nanosecond mtimes in the compressed inventory. No physical-power claim is made.

## Remaining scope

Declared metadata capabilities do not prove deployment configuration agreement,
full retired frame/operation/result/parent/source/lease history, scalar presence/
null admission, all-peer durability or original simulation/scale/fault/soak/
rollout acceptance. Full lower-version physical compatibility beyond these focused
controls remains separate. Four larger campaigns remain live under older frozen
inputs, which do not qualify this change. Public continuation admission/import/
online collection remain disabled.
