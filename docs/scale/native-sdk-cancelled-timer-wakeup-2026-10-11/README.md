# Cancelled timer wakeups after archived SDK checkpoints — 2026-10-11

Real R1/R3Domain cases run production SDK timer workflows, continuation frames,
native graph storage, checkpoint compaction and object collection. The first
checkpoint cancels absolute timer4; the next carries4 and adds14. After each
archive sweep and boundary scan, independently fenced deliveries inject cancelled
timer wakeups for the current invocation. The supplied timestamp is two hours in
the future, so wall-time due status cannot bypass the persisted cancellation.

At next, step4 must be a no-op. At finish, both4 and14 must be no-ops. Each test
requires the cancellation marker, byte-identical complete native logical history,
unchanged handler counts and unchanged effects. It then resumes the next real
continuation and completes with43, two effects and one call per handler. The final
independent raw graph/journal/checkpoint audit reconstructs the cancelled timer
sets through the archive and live suffix.

```sh
WF_GRAPH_SDK_DIAGNOSTIC_ROOT=/home/exedev/js-wf-cancelled-timer-wakeup-20261011 WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker -run '^TestNativeGraphContinuationCancelledTimerWakeup$' -count=1 -v -timeout=4m
```

review.json records actual process exit/timing, six no-op receipts, source hashes,
actual race executable and retained closed stores. CI requires both cases and all
six cancellation receipts; its guard is executed against the retained log.

## Remaining scope

Cancelled wakeups are injected at the production worker execute boundary. This
proves execution suppression/immutable logical history, not broker timer delivery,
RunPartition ACK metrics or physical object inventory stability. Healthy native
scheduling of positive timers is qualified separately. Tagged-clock bounds, early/
late clock repair, process-kill cuts, physical timer mutations and original scale/
fault/soak gates remain open. Earlier unconfirmed lease initialization failure is
not resolved or replaced by this campaign. Public admission/import/collection
remain disabled; frozen larger campaigns retain their earlier sources.
