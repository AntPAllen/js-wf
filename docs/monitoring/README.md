# Journal capacity monitoring

`wf-worker -metrics-addr 127.0.0.1:9091` exposes worker metrics and the actual
`WF_JRN` byte usage/limit. Provision a positive journal byte cap using the
worker's `-journal-max-bytes` option. An unlimited journal has no capacity ratio.
For an embedded worker, mount `worker.MetricsHandler` and add journal capacity
samples from `provision.CheckJournalCapacity` to your application's exporter.

The shipping rule alerts when the cluster's maximum journal utilization stays
at or above 70% for two minutes. The samples here connect Prometheus to
Alertmanager and enable both firing and resolved webhook notifications. Change
the cluster label, metrics targets and receiver URL for your deployment.
The receiver must implement Alertmanager's webhook contract; a JSON field named
`status` distinguishes firing from resolved. Keep credentials in your deployment
configuration rather than repository examples.

On Linux, run the worker and receiver on the configured local ports, then start
these processes with Docker host networking. The configuration directory is
mounted read-only. The images are the versions used by the delivery fixture.

```sh
docker run --rm --name js-wf-prometheus --network host \
  --mount "type=bind,src=$PWD/docs/monitoring,dst=/etc/prometheus,readonly" \
  prom/prometheus:v3.15.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.listen-address=127.0.0.1:9090

docker run --rm --name js-wf-alertmanager --network host \
  --mount "type=bind,src=$PWD/docs/monitoring,dst=/etc/alertmanager,readonly" \
  prom/alertmanager:v0.34.1 \
  --config.file=/etc/alertmanager/alertmanager.yml \
  --web.listen-address=127.0.0.1:9093 --cluster.listen-address=
```

Run each command in a separate terminal. Your receiver is a required application
service; these examples do not supply or configure a production notification
account. Configure monitoring storage and process supervision for your own
retention/restart requirements.

## Delivery proof

The opt-in test starts a real three-replica NATS journal, mounts the production
metrics handler, and starts real Prometheus and Alertmanager containers with a
private webhook receiver. It copies the shipping rule without changing its
threshold or two-minute hold. It verifies no alert below threshold, observes the
pending rule after filling the journal, receives a correctly labeled firing
notification after the hold, removes only fixture data, and receives resolution.
It does not synthesize alerts or post them to Alertmanager's alerts API.

```sh
WF_MONITORING_ALERT=1 WF_MONITORING_ARTIFACT_DIR=/tmp/js-wf-alert-proof \
  GOMEMLIMIT=512MiB GOMAXPROCS=2 \
  go test -p=1 -race ./cmd/wf-worker \
  -run '^TestJournalCapacityAlertDeliveredAndResolved$' -count=1 -timeout=6m -v
```

Artifacts include loaded configuration, pending alert state, raw webhook bodies,
container image identities and server logs. The `monitoring-alert-delivery`
workflow runs the same fixture. This proves local rule/receiver integration;
operator-specific routing, credentials and notification delivery remain deployment
configuration.

Configuration fields follow the official [Prometheus configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
and [Alertmanager configuration](https://prometheus.io/docs/alerting/latest/configuration/).
