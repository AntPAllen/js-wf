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

The initial full run passed on the expanded VM. Creating the fixture took
4m35s; the cursor advanced from 129 to 257 across the process kill, and the
whole test finished in 5m10s. That run did not traverse the full stream.

The scanner now inspects up to 32 invocations concurrently within each bounded
page, then orders candidates and cursor advancement by stream sequence. A
full million-entry sweep after leader takeover passed in 4m51.7s with a
1,024-sequence page budget and 10 ms loop cadence. Caching successful
`WF_JRN` and `WF_STATE` handles per journal store removed repeated stream-info
lookups without caching journal contents. With that change, a second full run
created the fixture in 2m38s and swept the million retained waits in 3m25.6s;
the entire test took 6m38s. A 1,000-invocation race-instrumented full-sweep
diagnostic also passed.

This proves a complete cursor traversal within five minutes after the scanner
starts on this VM, including takeover from a SIGKILLed leader. It does not
prove the plan's end-to-end five-minute completion bound from each wait's
enabling event, because the fixture is built before scanning starts and no
signals are sent. It does not cover the full fault matrix.

```sh
WF_SUSPENDED_SCAN_SCALE=1 WF_SUSPENDED_SCAN_FULL=1 go test ./integration \
  -run '^TestMillionSuspendedScanCursorSurvivesLeaderKill$' \
  -count=1 -timeout=15m -v
```

Set `WF_SUSPENDED_SCAN_COUNT=1000` for a smaller diagnostic run. Omit
`WF_SUSPENDED_SCAN_FULL=1` to check takeover without timing a complete sweep.
