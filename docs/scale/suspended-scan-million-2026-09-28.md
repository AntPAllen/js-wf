# Million-suspended reconciler cursor proof

`TestMillionSuspendedScanCursorSurvivesLeaderKill` creates one million distinct
invocations on a three-node JetStream cluster. Each has a real three-entry
journal: `Started`, a request for signal `go`, and `Suspended{waiting_on:
"signal:go"}`. The fixture uses the production `journal.Append` CAS path and
checks that `WF_INV` retains one million subjects and messages while `WF_JRN`
retains one million subjects and three million messages.

It then starts a suspended reconciler loop in a separate process, waits for
the persisted `WF_STATE` cursor to advance, SIGKILLs that process, and starts
a replacement loop on another cluster node. The test polls the cursor and
rejects any backward move. The replacement must advance it by at least 128
more invocation sequences after the original leader's 30-second KV lease
expires. No signal is published, so the scan should not enqueue a wakeup.

The full run passed on the expanded VM. Creating the fixture took 4m35s;
the cursor advanced from 129 to 257 across the process kill, and the whole
test finished in 5m10s. A 1,000-invocation diagnostic passed in 34 seconds.
This proves persisted cursor takeover with a million suspended invocations
retained. It does not prove that the scanner can traverse all million within
the five-minute liveness target or cover the full fault matrix.

```sh
WF_SUSPENDED_SCAN_SCALE=1 go test ./integration \
  -run '^TestMillionSuspendedScanCursorSurvivesLeaderKill$' \
  -count=1 -timeout=15m -v
```

Set `WF_SUSPENDED_SCAN_COUNT=1000` for a smaller diagnostic run.
