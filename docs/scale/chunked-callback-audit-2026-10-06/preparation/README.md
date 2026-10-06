# Explicit chunked callback candidate

Cold full-capacity CPU evidence motivates amortizing adapter waits. The candidate
moves up to 256 messages at a time into reader-owned storage. Each of the pending
and active buffers is bounded to 256 records / 1MiB payload, or one oversized
record; SDK 8MiB buffering, in-flight callback payload, headers and objects are
separate. The first record wakes the reader, so tails need no flush timer.
The SDK handler only enqueues; invariant visitors remain on the scanner goroutine.

Public/default readers are unchanged. Explicit WF_AUDIT_CHUNKED_CALLBACK=1 selects
the candidate in the R1 full-oracle and full400k fault harnesses. Common gap, order,
semantic error, confirmed position-loss recovery, two-resume, and cleanup logic
remain shared. Original20s / full400k / 4.8M entry gates are unchanged.

Race controls cover record/byte pressure, oversized payloads, exact ordering,
pressure shutdown/join, cancellation and typed errors ahead of buffered records.
Native legacy full-oracle comparisons precede a fresh full400k CPU/fault gate.
Only recorded executed-source evidence can establish acceptance. No speedup,
capacity, fault or default/24h claim is made from preparation.
