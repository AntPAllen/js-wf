# Start scan partial-timeout progress

The pre-fix reproduction fails seed2 after48 virtual seconds: cursor1 and no
repair. A new optional error checkpoint preserves a confirmed prefix while
retaining the failing invocation. Fresh-context renewal and cursor CAS are
required; lost ownership, cancellation and fatal errors cannot checkpoint.

128 fixed seeds exercise checkpoint versus legacy discard, request100/150ms,
cursor commit/drop/lost-ack and enqueue ack/lost-ack. All12 policy cells replay
and are pinned. The loop uses its modeled30s lease; the real contract uses the
production12s lease. These cases prove partial-timeout progress, not arbitrary
population/latency bounds or the earlier R5 server/cursor cause.

Normal model0.048s, full pins0.550s, model/pins race7.999s. Three-node real contract
3.374s; focused reconciler race4.353s and full package14.609s. The real cursor
advances1→4→7→8 with exactly one retained run after an actual hidden enqueue ack.

The archive includes raw failed/successful logs, saved pre-fix trace, original
production sources, corrected sources and12 pins. It was reopened and every
member hashed before atomic rename. The pre-fix build added only the inert
RetrySequence field and new reproducer to the original loop/scanner behavior;
it did not retain its test executable, so no exact-binary provenance is claimed.
Rejected startup/port-initialization fixture logs are kept separately from the
runtime counterexample. Clean committed full1k qualification remains separate.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 ./sim -run 'Test(StartPartialCursorReplay|PinnedRegressionCorpus)$' -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./reconcile -run '^TestStartPartialCursor' -count=1
```
