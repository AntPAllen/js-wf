# R5 all-server SIGKILL/restart race smoke

Named native race test PASS 64.06 s; public Go test2json package PASS 65.095 s.
Five batches, 140 terminal invocations, 1,545 journal entries and one complete
five-server SIGKILL/restart boundary. All six per-type raw enabling/terminal
p99 gates, client histories, retained invariants, physical WF_RUN and all64
consumer drain checks pass. Largest terminal p99 16.406241189 s; largest
progress p99 14.31259829 s. Production sync_interval remains2m.

The operation artifact records each literal SIGKILL's completed container
removal before any restart; node4 removal at18:53:19.906205815UTC precedes
node0 completed restart at18:53:20.233400616UTC. The code completes all kills
and persists that boundary before calling RestartNode. Stable published client
and monitoring endpoints are checked; all stores recover current R5 peers.

All97 acknowledged repair attempts have checked explanations (28 start,
63 signal,6 suspended); fencing count is zero. Startup had two bounded metadata
request timeouts before successful provisioning, retained in the original log.
No server-side cause is claimed. All36 Python tests and integration vet pass;
controls reject rolling/partial restarts and false timestamp/node evidence.

Binary built from d91e64b plus the new uncommitted restart implementation.
The retained source patch includes subsequent comment and skip/failure-message
wording edits; operational Go logic is unchanged from the tested binary.
SHA256, raw native events and terminal user-service state are retained.

This35-second fixture smoke does not clear the ten-minute row, the separate
mid-fan-out restart property or the full24-hour matrix. Original server stores
remain outside Git; large raw files are losslessly compressed.
