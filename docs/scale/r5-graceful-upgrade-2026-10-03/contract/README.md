# Five-peer graceful rolling upgrade contract

Clean sourcecd59b86556cc04bc9524119dee6a7720a78ed32c passes the actual Docker
contract under race instrumentation in79.348s package/78.32s named test.
All five peers start2.11.17 and upgrade exactly once in order2,0,4,1,3 to2.15.0.
Initial32 messages plus one publication per upgrade yield37 immutable payloads;
every complete round reports the matching message count and last sequence on
all five physical replicas. The actual Go test reads and compares every payload.

Each old process receives SIGUSR2 with30s configured Lame Duck duration and10s
grace. Direct pinned-client notifications identify entry. Retained old-process
logs contain ordered entry, shutdown and exit; exact-name Docker state observes
exit/removal before replacing the binary on the same store. Five observed
shutdown/removal intervals are10.464–12.042s. The duration configuration spreads
client eviction; it is not a required minimum shutdown time for these few clients.

Two compiled controls execute the same named test:

- Single-old-peer constructor: actual failure6.034s at initial node1 version.
- SIGKILL substituted for SIGUSR2: actual failure9.351s at missing Lame Duck
  notification for node2. The original observation field is insufficient to
  pass when the real server does not send the notification.

Build failures, skips, unrelated failures and timeouts do not count. All591
recorded source hashes match the exact Git revision before and after execution.
Independent review verifies precise overlay derivation, named/package verdicts,
every shutdown timeline and six complete rounds of five physical snapshots.

The archive retains commands, raw Go JSON, logs, before/after source hashes,
selected exact source bytes, precise controls, observations, physical stores,
old/current binaries and the review utility. Every member SHA256 is checked by
reopening the temporary archive before atomic rename.

This is the constructor and retained-data upgrade contract. Sustained graceful
mixed workload,200 seeds,24-hour matrix and forced two-write-gap coverage remain
open. Earlier SIGKILL qualifications retain their independent scope.
