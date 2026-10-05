# Copied state watch recovery with the matrix client policy

Executed source: `06a34afdaf24931a5ca843691efdda516cbe819c`.

The focused native test passed in 14.49 s. Its retained-state snapshot completed
in **2.241321269 s**, returning all **29,177 values**, within the existing three
2-second calls and 20-second outer deadline. No production budget changed.

The newly created R1 memory / AckNone watch consumer had 27,221 pending values
when its leader (node 0) was selected for independently observed SIGKILL. The
first watch delivered 11,685 values without its initial completion barrier and
expired. The second watch completed its initial set with all 29,177 values.
The client subsequently reported five mapped loopback endpoints, connected to
one of those endpoints, with no discovered endpoints. Its policy matches the
real matrix parent: IgnoreDiscoveredServers, 1 s dial timeout, 100 ms reconnect.

Independent review verifies the actual SDK executable and clean Git build,
657 selected source inputs before/after, all five actual Docker process
executables/build fields and copied-store mounts, and 3,706 original store files
against the complete original archive. Original store bytes remain unchanged;
only disposable copies were opened. The killed server log is explicitly pre-cut;
the other four server logs were captured after the snapshot.

All **4,194 archive members** and **eight parts** were read back and verified.
Archive SHA256: `776c9c0a22482472e9c7cd40832a91b35d4f6030d8d49a00cbfc2249a068086c`.
Producer, independent reviewer, preserver, source, executable bytes, native logs,
and copied stores are retained in the archive.

This qualifies only the focused copied-state consumer leader-loss case. The
previous copied fixture failed at a different killed leader and did not record
its endpoint pool. This comparison does not prove discovery caused that failure.
The real matrix already used the corrected policy: this is not a fix or cause
confirmation for checkpoint 1040, a concurrent runtime workload, a complete
retained journal/state audit, the full matrices, or the actual 24-hour gate.
The earlier failed originals remain available in the parent directory.
