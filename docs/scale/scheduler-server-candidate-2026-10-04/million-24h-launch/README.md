# Full million-timer/24h candidate comparison: verified launch

Persistent unit `js-wf-native-million-scheduler-candidate-24h-20261004.service`
executes247eeff from an isolated clean checkout. All1,000,000 scheduled publishes
acknowledge. Profile:24h deadline span,15m loading runway,64 publishers, file/R3,
original raw p99<2s/max<30s and both all-node SIGKILL cuts at the existing thirds.
First/last due:2026-10-04 23:31:49.944102316UTC /2026-10-05 23:31:49.944102316UTC.
SDK usesGOMAXPROCS2/GOMEMLIMIT1GiB. Snapshot receives4,275
timers; this is a live launch/load observation, not a terminal campaign pass.

Actual SDK PID215956 /SHA `659920c096170bbfba8fef858af8f683117d3f31af92168199e4dd0434d22550` and
all three live native executables/build-info fields verify. Candidate SHA
`ff7335643d02125ec50b8abaa0ca80fe4da36473aa14402c2dbcf029e63c9fe8` is the previously preserved single-file dirty-count
control. All1,693 selected Go/module inputs and83 local Git inputs verify;
source captures and actual SDK/candidate bytes are retained. These are selected
Go/module inputs, not exhaustive assembly/embed/hermetic provenance.

All1712 snapshot archive members/2 parts read back.
Size:28,845,056 bytes; SHA256 `dd54be6ddf15f574599550ca813fb0bc917a4500ad08abba491111f52d1c2168`. Concatenate sorted
parts, verify the manifest, then extract fresh. Only immutable launch snapshots
are archived; live stores remain at the original campaign root. Documented NATS
monitoring snapshots are observations, not independently reopened store proof.

The candidate is diagnostic and excluded from production release verification.
A completed million comparison still requires independent receipt/latency/physical
drain review and a separate adoption decision. Original million/24h distributed
soak/full matrices remain open; the earlier300-timer compressed run remains failed.
The launch overlapped the separate journal-soak audit failure, recorded with its
[complete failed originals](../../local-r5-streaming-audit-2026-10-04/explicit-routes-normal-2g-24h-failed/).
