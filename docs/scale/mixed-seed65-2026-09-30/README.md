# Mixed seed 65 latency failure

[Run 36786018726](https://github.com/AntPAllen/js-wf/actions/runs/36786018726)
ran clean source `a1995cc5de070842ae775bbac32fd2b1abd4f46d`. Seeds 1–64
passed; seed 65 failed the terminal p99 gate at 37.809095249 seconds across
28 observed terminal invocations. The signal parents took 37.809095249 and
33.676200557 seconds. This is the focused four-fault mixed campaign, not a
200-seed sustained full matrix. It does not establish a clean release gate.

The schedule combines persistent 85 ms syscall disk slowdown on node 0,
route isolation of node 1, SIGSTOP of journal leader node 2, and SIGKILL of
node 1. Disk slowdown remains enabled until cleanup. This is not the separate
sustained row's five-second block-device stall.

Run `python3 analyze.py` to reproduce operation accounting from the complete
1,928-event JSON, including operations that precede slow-operation log enabling.
The two signal parents have 52/51 unconditional append-renew observations, no
renew errors, and 35.554/33.243 seconds inside KV update calls. Local append
lease-gate waits total just 57/65 microseconds. These observations locate the
client wait, but do not establish a server-side cause. Concurrent acquisition
and heartbeat durations must not be added to these sequential append totals.

The failure occurs before the final integrity audit; terminal observation of
28 results is not proof that the final audit passed. The full raw failed-case
log, schedule, operation history, server logs, initial JetStream/Raft metadata
and timestamped disk trace are retained here. No success or causal fix is
claimed. The existing unconditional pre-append lease-renewal policy remains.
