# Journal capacity alert delivery

A real R3 NATS journal supplies the production wf-worker metrics handler. Actual Prometheus v3.15.0 and Alertmanager v0.34.1 containers evaluate the unchanged shipping rule and send notifications to a private HTTP receiver. The test does not synthesize an alert or post to Alertmanager's alerts API. Container image identities, exact configs, pending alert state, server logs and raw webhook bodies are retained.

Race PASS134.77s test /135.800s package. Below-threshold scrape succeeds with no notification. A real journal fill uses12,339 of16,384 bytes (75.3113%). The rule is pending, then the firing webhook arrives120.271080907s after fill, preserving the shipping two-minute hold. Severity warning, cluster and alert labels, annotations and alert identity are checked. Purging only the fixture subject produces a resolved notification1.000649782s later and zero utilization. The production example configurations pass promtool/amtool validators. Vet passes. The ordinary test invocation skips without explicit opt-in.

A no-hold control changes only the fixture-loaded rule from2m to0s. Actual firing is delivered prematurely; the pending-phase requirement fails in23.26s test /23.274s package. It is a semantic rule-phase rejection, not a process or broad test timeout. Both control logs and its firing webhook are retained.

The first attempt used the four-minute fixture context for provisioning and failed there after240.01s, before monitoring processes began. It is retained and excluded. The final fixture limits provisioning to30s and individual attempts to3s; one timed-out attempt is followed by a successful1.125s attempt. This does not establish a server-side cause for the original missing response.

This proves local capacity-rule/receiver integration. Operator-specific production accounts, credentials, endpoints and monitoring storage/supervision remain deployment configuration. It does not clear the runtime fault matrix or release soak.
