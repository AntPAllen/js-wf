# Runner waits for asynchronous fallback timer cleanup — October 1, 2026

Standard CI at 87d489a failed the fallback-runner fixture after returning the
correct workflow result 42: its immediate WF_TIMER read still had one source
record. The original log does not identify the exact interleaving. Production
fallback scanning publishes a durable wakeup before deleting its source; observing
a terminal result does not establish that deletion has finished. Another ready
wakeup can also let the SDK complete a due timer independently of that scanner.

The fixture now requires cleanup within three seconds before stopping the
scanner. It retains the successful result, explicit fallback provisioning and
joined runner shutdown assertions. Production scan/write/delete ordering is
unchanged; no workflow latency or fault-recovery target is relaxed.

Compiled controls establish both boundaries:

- Hold DeleteTimer for 500 ms: the original immediate assertion fails with one
  timer, while the revised fixture passes and observes removal.
- Return success without DeleteTimer: the revised fixture fails after its
  bounded cleanup deadline with one source record. It cannot silently accept
  permanent retention just because the workflow completed.

Old-delay failure took 47.433 seconds, delayed-delete pass 45.870 seconds and
missing-delete failure 47.128 seconds; those process totals include cold plugin
builds, not timer latency. The full worker-runner package passed under race in
35.739 seconds. Vet passes. These are compiled semantic controls, not build
failures or process-level timeouts. Original CI log, logs, patches and source
hashes are retained. The controlled delay proves the assertion was too early;
it does not establish the original hosted interleaving's cause.

Fresh standard CI confirmation remains pending. Full mixed matrix/24-hour soak,
independent mixed latency failures, and remaining implementation-plan gates remain
open. The original million-timer and full simulation processes have not restarted.
