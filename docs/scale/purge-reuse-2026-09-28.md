# Concurrent purge and ID reuse

The opt-in `TestTenThousandConcurrentPurgesAndThousandReusedIDs` runs on a
three-node JetStream cluster with six workers. It completes 10,000 original
invocations, then starts 10,000 other invocations and waits for each to
journal a signal suspension. While those 10,000 remain live and nonterminal,
64 callers purge the completed generation of every original invocation. The
first 1,000 purged IDs are started again before the other purges finish; the
test asserts that this overlap actually occurs.

For each reused ID, the test publishes a delayed signal carrying the old
invocation sequence before starting the new generation. The new workflow
must suspend without consuming it. A fresh signal then completes it with the
new payload. Every one of the 1,000 reused journals is read and checked:
index zero is `Started`, all entry epochs are newer than that ID's old
generation, exactly one fresh signal was consumed, and the terminal outcome
names the new invocation sequence. The 10,000 unaffected workflows are
still suspended after the purge; all are then signaled, completed, and their
results checked. `WF_RUN` drains, retained `WF_INV` has exactly 11,000
subjects, and the raw-stream integrity checker passes.

Two full 10,000/10,000/1,000 runs passed on the expanded VM. The second run
completed the overlapping purge/reuse phase in 28.7 seconds, checked all
results by 47.1 seconds, and completed the integrity audit by 75.6 seconds.
A 100/100/10 race-instrumented run also passed. The test's live workflows
are durably suspended while purges run; it does not prove 10,000 handlers
are simultaneously executing. The runtime uses KV lease creation revisions
as fencing epochs, so a reused journal begins at index zero with a new
positive epoch instead of the plan's literal `epoch: 0`.

Reproduce the full proof with:

```sh
WF_PURGE_REUSE_SCALE=1 go test ./integration \
  -run '^TestTenThousandConcurrentPurgesAndThousandReusedIDs$' \
  -count=1 -timeout=25m -v
```

Set `WF_PURGE_REUSE_COUNT=100` for a smaller diagnostic run.
