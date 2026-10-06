# Full six fanout cuts with local physical drain

Full original R3 file-storage race campaign at `cdd4c7b` passes in **525.46 s**:
create first/interior/last (69.93/137.14/101.75 s) and results first/interior/last
(70.71/75.29/70.63 s). Each retains 500 child results, parent result 249500,
501 invocation/journal/terminal records and the exact pre-SIGKILL journal prefix
across an actual parent SIGKILL and library journal-leader restart.

Actual SDK PID 2293554 is closed; its SHA256 is
`d2a0796b7c88e4a669c4a09cebef199aa2cc8432aac06ab5d3ebed38c995024b`.
Captured source/module 696 files and external 3,287 files, producer, actual killed
parent executables, race/2CPU/2GiB profile and original five-minute case deadlines
independently recheck. Servers are embedded in the actual SDK; no separate server
process identity claim is made. All eighteen worker starts succeeded on their
first attempt: this run does not exercise transient startup retry recovery.

All six cases retain three distinct public per-server Jsz local WF_RUN states,
zero local messages/64 consumers and all 64 zero-pending/zero-ack-pending durable
states after all 64 drain workers joined, within each original case deadline.
Full archive: 16,805 members / 69,051,153 bytes / three parts; all read back.

This qualifies the full six native cuts with stronger local physical witnesses.
Independent fresh copied audits are pending. The prior failed campaign and
unconfirmed underlying constructor timeout cause remain preserved; full fault
matrices and actual 24-hour gates remain separate.
