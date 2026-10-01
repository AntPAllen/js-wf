# R5 majority-progress route row: race smoke

Native race PASS71.70s; public Go test2json package PASS72.732s. Seven batches,
196 completed invocations,2,173 journal entries and one confirmed single-node
route partition. All histories, retained invariants and physical WF_RUN/all64
consumer drain pass. Largest raw terminal p99 is15.397236222s (grandchild),
largest progress p99 is17.389230967s (timer). There is no heal-time adjustment;
the four-node majority remains available.

The isolated node differs from the shared client/worker connection's current
server. It reports zero routes. The separate R5 probe acknowledges a new
sequence during the cut, and WF_JRN advances from2008 to2081 before reconnect.
The artifact checker ties these observations to the confirmed cut/heal times,
checks distinct client/isolated endpoints and the matching fault sequence,
and requires all five nodes to restore at least16 routes and current R5 peers.
This uses1s fixture route ping, route-only advertised aliases and production2m
write sync. Five worker objects share one connection on the majority side.

All six fencing events match worker counters and lose heartbeat ownership
confirmation during the partition. All six invocations were already terminal
before duplicate delivery fencing; the review retains their earlier terminal
samples, exact delivery traces and later invocation acknowledgements. These
later cleanup acknowledgements are distinct from completion. No exact server
missing-response mechanism is asserted. All70 acknowledged repairs (32start,
31signal,7suspended) have checked source/decision explanations. All39 Python
tests and integration/testcluster vet pass. Controls reject missing journal
progress, post-heal observations, isolated-client selection, partial/incorrect
route cuts, probe failures and sequence mismatches.

Source isf8939b1 plus the retained patch; tested Go files and binary are hashed.
This35s smoke does not clear the ten-minute row, separate worker processes,
default-ping variant or full24-hour matrix. Original stores remain outside Git;
large raw artifacts are losslessly compressed.
