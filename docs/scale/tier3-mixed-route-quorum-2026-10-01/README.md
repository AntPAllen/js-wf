# Sustained R5 quorum-removing route row: race smoke

Native race PASS72.43s; public Go test2json package PASS73.454s. Six batches,
168 invocations,1,852 entries and one confirmed quorum-removing cut. Three
servers were disconnected from the route network, each reported zero routes,
and the separate R5 probe publication returned an unacknowledged error. All
five servers later reported at least16 routes, workflow R5 peers recovered,
and a probe publication acknowledged before final heal was recorded.

All histories, retained invariants and physical WF_RUN/all64-consumer drain
checks pass. Raw and recovery samples are both retained. Largest raw terminal
p99 is23.723548034s (fan-out), and largest recovery terminal p99 is5.006226773s.
All792 samples independently match their recorded enabling/observed/heal
nanosecond timestamps and the reported raw/recovery p99. Only intervals that
overlap an outage use the later enabling event or last overlapping confirmed
heal; healthy intervals retain their raw delay and completion before heal
contributes zero recovery delay. Raw32s/recovery1s control passes, whereas
falsified recovery delays, timestamps, route counts, cut cardinality, missing
probe outcomes and incorrect p99/counts fail. All38 Python tests pass. The
focused Go recovery race test and integration/testcluster vet pass.

All59 acknowledged repair attempts (10start,39signal,10suspended) have checked
source/decision explanations; fencing count is zero. No precise server-side
missing-response mechanism is claimed.

This fixture explicitly uses1s cluster route ping, route-only advertised
aliases, and production2m write sync. Two prior attempts with default route
ping failed the30s zero-route observation deadline:80.06s/80.080s and76.01s/
76.032s. Neither established the cut or clears acceptance. Their raw evidence
is retained in failed-* directories. The pinned server caps default route ping
at30s; the1s fixture interval makes stale-socket detection observable within
the fault cadence. Alias selection and connection timing were not independently
isolated as causes of the previous retained counts.

Binary source isdae2c91 plus the retained tracked patch and new Go sources.
Original stores for all three attempts remain outside Git. This35s smoke is
not a ten-minute row, a default-route-ping acceptance test, the majority-side
progress row or the full24-hour matrix.
