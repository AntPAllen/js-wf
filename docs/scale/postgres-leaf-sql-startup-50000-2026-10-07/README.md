# Packaged SQL startup cancellation followed by original full50000 recovery

Prepared, not yet accepted. New `standalone-leaf-startup` runner profile executes `TestStandalonePostgresProjectionCrashAndSessionLossFiftyThousandInvocationsThroughLeafWithSQLStartupCancellation`.

A real PostgreSQL transaction holds an exclusive table lock. PostgreSQL's actual backend lock graph must show the packaged child waiting in schema initialization. SIGTERM must join the child with exit0 within ten seconds while the blocker remains held; all child SQL sessions and locks must be gone, the table must remain empty, and neither projection durable may have been created. Full child process identity and complete stock-leaf wire are retained. The original three packaged phases, all50000 invocations, fault/rebuild assertions,20m case/22m SDK/count1/nonrace/twoCPU2GiB then run unchanged.

The independent reviewer requires exact actual lock/query identity, ordered blocking/signal/reap/residual/release/next-child timestamps, both expected startup stream-info requests with no later NATS operation, clean exit and complete actual wire. Existing accepted leaf proof remains bound to its recorded source and original scope. The gate-disabled compile check is not native acceptance. Production code is unchanged pending native evidence.
