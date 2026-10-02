# Independent clock sampling foundation

The full runtimeclock race suite passes in 1.139s. The native three-node contract
passes in 3.37s (4.394s package). It uses documented server tags to place three
single-replica probes on distinct physical servers, then samples all through one
client. It confirms the actual response identities (not the connected peer),
142ms uncertainty, two same-owner probe aliases count once, copied trusted
topology, and rejection of the real replicated WF_RUN stream.

Unit checks cover ±60s clock shifts, a canceled/unavailable source, single-owner
migration, malformed aliases, cancellation, bounded reading age, immutable
source ownership and missing/untrusted/replicated/migrating response provenance.
Existing exhaustive estimator tests also run. Commands:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./runtimeclock -count=1 -timeout=2m
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -p=1 -race ./integration -run '^TestRuntimeClockSamplesForwardedIndependentLeaders$' -count=1 -timeout=3m -v
```

`manifest.json` hashes the tested code and complete logs. The logs are terminal
PASS outputs from these executions. A prior development test used replicated
probes; it is not acceptance evidence for this final source.

Source inspection of the pinned NATS server v2.15.0 shows that STREAM.INFO
leader admission and cluster metadata construction are separate. Therefore a
replicated response cannot safely authenticate the timestamp producer solely
through its reported current leader. The adapter requires a single replica,
no Raft group, no replica list and no desired migration. The R1 cluster-info
path reports the responding server's own name. Trusted operator topology maps
that name to a physical identity; an authenticated cluster and protected reply
subjects remain prerequisites. Standalone responses without cluster provenance
are rejected. No system-account request or infrastructure endpoint is used.

This adds usable production transport code, not an end-to-end timer fix.
Production probe provisioning/topology management, durable clock-domain markers,
canonical deadlines, scheduling translation, SDK due checks, repair changes and
legacy migration remain open. The native ahead-clock 60.27s progress failure is
unchanged, and no clock or release gate is relaxed by these tests.
