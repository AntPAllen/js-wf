# Native five-container independent clock topology

The Docker fixture accepts copied per-node server tags through
`StartDockerClusterWithStoresAndTags`. Documented NATS `server_tags` configuration
is mounted separately for tagged nodes and regenerated identically on restart;
existing untagged constructors retain their original shared configuration.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./testcluster -run '^TestDocker(Tags|NodeConfig)' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./reconcile -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 WF_TIER3_CLOCK_PROBES=1 TIER3_CLOCK_PROBE_ARTIFACT_ROOT=/tmp/js-wf-clock-probes go test -race -p=1 ./integration -run '^TestFiveContainerIndependentClockWithSkewAndProbeRestart$' -count=1 -timeout=8m -v
```

Tags/parser/copy/restart-config tests pass1.016s. Full reconcile race passes
13.346s with the new observed-loop clock entry point; existing callers retain
the nil clock. The new entry point keeps both scan and repair observers plus
fenced lease/cursor handling. Native elected-loop domain use is not yet covered.

Native five-container probe tests pass37.25s / package38.262s: actual server4
clock shifts of ±60s, exact five R1 placements, fresh canonical bounds with all
probes, with healthy node0 killed, and after node0 restart. The restarted probe
returns its configured physical identity. An actual shifted probe timestamp is
checked independently. Refresh1ns forces fresh collections; canonical intervals
intersect the controller observation interval and remain at most1s wide.
Topology/proofs, node configs and replacement server logs are retained as gzip;
all uncompressed hashes and sizes were verified before committing.

The initial attempt failed its immediate3s post-restart metadata read in both
directions after the loss-time clock check succeeded. The final test retries
transient metadata errors within a30s readiness window and logs them; each
direction has one timeout before success. This diagnoses readiness, not permanent
probe loss, and does not change worker recovery or timer p99 targets. Initial
source hashes and full failed log are retained.

These are five server instances on one host, with one server-specific clock
overlay. Physical identity and healthy-clock bounds remain operator assumptions.
This is topology/sampling/restart evidence, not workflow recovery acceptance.
The R5 mixed fixture still uses legacy deadlines. Its origin/admission and
controller audit must preserve both canonical deadline and shifted native
scheduling proof before tagged writers can be enabled. Ahead60.27s and incomplete
behind sustained gates remain open; full release scope is unchanged.
