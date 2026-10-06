# Operator CLI explicit JetStream domain

`wf -domain NAME` selects NewWithDomain for online operator commands; omission preserves the default context. Offline bundle replay remains independent of a connection.

The existing operator command fixture now runs in the default single-node cluster and a real three-node WFOPS domain. It covers assignments/CAS, list/paging/rebuild, lag, suspended scans, describe, cancellation/purge, export and actual handler-plugin replay including spilled objects. A missing domain must fail. Domain startup retries metadata under30s with4s attempts, within the original60s body/3m SDK limits. All three actual peer domains/server IDs are admitted.

Exploratory logs preserve the provisioning timeout and server-subscription observation failure. NATS maps incoming domain API subjects before ordinary subscribers see them. Client RequestSent tracing now verifies the outgoing administrative prefix on the same operator path. The uncommitted race pair passes (default3.38s/domain7.70s;203 observed domain requests/zero wrong prefixes) and five raw-log substitutions are rejected after checking the actual terminal Go package success. Preparation is not clean-source/full-originals acceptance.

The reusable runner retains selected sources, actual race SDK profile, replay plugin, stores/bundles/logs and a verified complete archive. WF_OPERATOR_TEST_ROOT retains fixtures/plugin outside automatic cleanup. Clean native qualification remains pending; no PostgreSQL/domain, fault matrix, million drain or actual24h qualification is claimed.
