# Operator CLI explicit JetStream domain

`wf -domain NAME` selects NewWithDomain for online operator commands; omission preserves the default context. Offline bundle replay remains independent of a connection.

The existing operator command fixture now runs in the default single-node cluster and a real three-node WFOPS domain. It covers assignments/CAS, list/paging/rebuild, lag, suspended scans, describe, cancellation/purge, export and actual handler-plugin replay including spilled objects. A missing domain must fail. Domain startup retries metadata under30s with4s attempts, within the original60s body/3m SDK limits. All three actual peer domains/server IDs are admitted.

Exploratory logs preserve the provisioning timeout and server-subscription observation failure. NATS maps incoming domain API subjects before ordinary subscribers see them. Client RequestSent tracing now verifies the outgoing administrative prefix on the same operator path. The uncommitted race pair passes (default3.38s/domain7.70s;203 observed domain requests/zero wrong prefixes) and five raw-log substitutions are rejected after checking the actual terminal Go package success. Preparation is not clean-source/full-originals acceptance.

The reusable runner retains selected sources, actual race SDK profile, replay plugin, stores/bundles/logs and a verified complete archive. WF_OPERATOR_TEST_ROOT retains fixtures/plugin outside automatic cleanup. Clean native qualification remains pending; no PostgreSQL/domain, fault matrix, million drain or actual24h qualification is claimed.

## Clean native race pair accepted

Actual clean source `ea29cf8e4ec6be78711e81cfbccbdb7075695d52` passes default3.84s and domain8.34s,13.2229s actual SDK total. The same command assertions verify assignment/CAS, list/paging/rebuild, lag, suspended scan, describe, cancellation/purge, export and real handler-plugin replay of inline/spilled data. The unknown-domain control returns an error. All three actual WFOPS peers are admitted; client tracing records203 administrative requests with zero wrong traced API prefixes. These are client trace observations, not an exhaustive count of all wire publications.

Independent review verifies1899 exact Git/before/after/current retained source inputs, actual SDKrace/count1/3m/GOMAX2/1GiB/GOWORKoff/emptyGOFLAGS, actual cmd/wf working directory, SDK hash/closed PID, loaded retained race plugin, two closed fixture store topologies1/3 and complete2409-member48,091,236-byte archive. Every member/hash/mode/mtime and current-root equality verifies. Two guard groups reject nine actual-log substitutions and the original failed runner.

The original wrong-cwd/missing-plugin case remains failed and preserved with verified S3 readback. The corrected runner records real cwd and archives missing-plugin failures. This qualifies the healthy operator domain feature and default command compatibility at the recorded source; daemon project behavior, PostgreSQL/domain, leaf-node routing, CLI fault cuts, full matrices, million timer drain and actual24h remain separate. See `native-race/independent-review.json`, original logs and complete archive metadata.
