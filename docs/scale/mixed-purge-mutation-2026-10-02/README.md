# Mixed production purge-order mutation

The shared fixture admits and completes four shorts, three timer parents, two
signal parents and a six-child/twelve-grandchild fan-out through a real journal
leader SIGKILL. Before retention starts, the original28 invocations pass the
whole retained-state audit, immutable-prefix checks and higher-epoch handoff.
Workers are joined before the target generation is purged.

Production retention runs through a JetStream wrapper that returns a specific
injected error immediately before the actual WF_SIG purge. All earlier writes
and reads use the real cluster. Both variants must hit this cut exactly once,
retain the committed generation-specific purge marker, and leave the original
four-entry journal and immutable terminal value/revision unchanged.

The intact implementation also retains the invocation at this cut. Retrying
without the wrapper completes retirement and an additional retry is idempotent.
The fixture verifies the invocation and journal are absent, the correct old
generation tombstone is present, and the transient purge marker is absent. A
production Start with changed input then reuses the ID. Replacement workers
write a fresh index0..3 journal entirely after the old physical tail; the
terminal outcome references the new invocation sequence. All28 current
generations pass another raw-state audit. The new generation keeps the runtime's
monotonic fencing epochs; no test resets or patches the lease counter.

The source mutant deletes WF_INV before the marker stage. At the injected cut,
the original invocation is absent while its journal, terminal state and purge
marker remain. An actual unwrapped purge retry returns retention.ErrNotFound.
Only that exact order/recovery failure triggers the semantic escape. The mutant
does not repair the acknowledged deletion, reuse the broken ID, or claim a
completed retirement. Timeout, unrelated error and compilation failure cannot
count as detection.

The final runner passes the baseline in59.781s and detects the mutant in49.547s
(including build/runner overhead). Test execution is49.30s and36.71s. The intact
race test passes in49.91s (50.943s package), including retirement, ID reuse and
the final28-generation audit, with no race warning. Both negative controls are
rejected. Independent checking decodes the raw invocation receipt, production
journal-read records and terminal KV bytes, binds their old generation, verifies
the tombstone and new physical journal interval, and checks actual named
pass/fail outcomes. Source/fixture hashes match the final worktree based on
`4e7de02` before commit. Original events/report are losslessly archived with
verified member SHA256 manifests; race events are losslessly compressed.

This supplies the sixth focused mixed source-mutation fixture. Hosted acceptance
is pending for the new mixed-purge job, as is the prior start-repair campaign.
These six focused mixed fixtures complement the six smaller contracts; they do
not clear the plan's sustained whole-chaos mutation campaign or full release gate.
