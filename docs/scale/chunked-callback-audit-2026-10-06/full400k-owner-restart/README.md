# Full400k same-store cursor-owner restart accepted

Clean8693ab5 normal4CPU/GOGC500/4GiB fresh full400k/4.8M R5 file-source copy.
Only the two verified old donor cursors are prepared/deleted; zero INV/JRN names/
counts and identical retained populations/bytes/first/last/deletion boundaries
recorded. Original1058 donor files remain unchanged. No journal warmup.

NativePASS53.85s. Exact healthy full report and4.8M once-only visits: audit
17.392763128s, zero consumers17.394841246s. Actual active R1 memory AckNone JRN
cursor owner node1 SIGKILL atvisit128, process absent, then same-node/data-store
restart. Distinct new cursor starts129; all4.8M entries visited exactly once,
exact full report, audit17.084632300s, zero consumers17.087980187s. Both old/new
cursor deletes succeed. All read/recovery/cleanup remains inside original20s.
This run resumes after transport failure; it does not exercise a fresh API proof
of volatile same-assignment position regression. Focused prior evidence remains
separate. Ordered cold/warmed stages are not an isolated speed ratio.

Independent review verifies actual SDK/688selected source inputs, six closed
server process observations including distinct old/new owner container/PID on
identical data mount, module/executable identities and preparation. Complete
1699-file816alias lossless reconstruction;50,248,507byte delta SHA256
`d7674535e7aee1892d612cef9dbbd52c1b4332ea7f7ac1d568ed5fac065cf879`.
Both pinned canonical donor Git parts and delta parts required. Fixtures stay
closed. This qualifies explicit fullsize same-store restart under recorded
configuration, following fullsize owner-left-down02d485a. Public/default/live/
currentmatrices/actual24h remain open.
