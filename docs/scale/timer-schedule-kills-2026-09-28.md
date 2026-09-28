# Timer publish crash boundary

`TestTwoHundredWorkerKillsBeforeTimerPublish` runs against a three-node
JetStream cluster. For each of 200 invocations it starts a real worker in a
child process. A test-only JetStream wrapper holds the child's scheduled
publish call and writes an atomic marker. The parent confirms that the
invocation journal contains `Started` and `StepRequested`, then sends
`SIGKILL` and verifies the child's exit signal. The wrapper has not forwarded
the publish. The parent purges the unacknowledged start wakeup before the next
child, leaving the journaled timer request as the durable recovery path.

After all 200 kills, `WF_RUN` contains no messages and `WF_STATE` contains no
results for these invocations. An `Await` without the timer reconciler times
out. The test then starts `RunTimerLoop` and three workers. All 200 results
are `true`, and every journal ends with `Completed`. Four full normal runs
passed; a full race-instrumented run also passed. The latest normal run took
about 44 seconds, and the race run took about 53 seconds.

The smaller `TestTwoHundredMissingTimerSchedulesReconcile` constructs the
journal state directly, then verifies the same recovery path. That test
passed three repeated normal runs and a race run. The process test provides
the plan's stronger evidence because it kills real workers at the precise
publish boundary.

Run the full process proof:

```sh
WF_TIMER_PROCESS_KILL_SCALE=1 go test ./integration \
  -run '^TestTwoHundredWorkerKillsBeforeTimerPublish$' -count=1 -timeout=7m -v
```

`WF_TIMER_PROCESS_KILL_COUNT` can reduce the count to diagnose failures; the
plan's gate requires the default 200. The test uses real elapsed time for
the 30-second lease TTL before recovery.
