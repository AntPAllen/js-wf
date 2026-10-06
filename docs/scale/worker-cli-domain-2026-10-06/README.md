# Worker CLI explicit JetStream domain

`wf-worker -domain NAME` selects NewWithDomain during every startup attempt. The same context supplies provisioned stores, assignment/membership, worker delivery, journal capacity metrics, retention and elected repairs. Empty selection preserves the default constructor. Independent clock-domain tagging remains a separate configuration.

Extend the existing workflow/metrics/retention assertions across static, KV and automatic assignment on three real WFWORKER peers, and the existing startup-retry scenario to an all-three-server library shutdown/restart. Original35s case/startup30s deadlines remain. Production leases and recovery targets are unchanged. Client request tracing rejects wrong administrative API prefixes; this is not an exhaustive wire publication count. WF_WORKER_TEST_ROOT retains current stores and actual loaded race plugin for full proof capture.

The first uncommitted race preparation passed default/domain execution and startup; the traced preparation and clean full-originals qualification remain pending. A dedicated two-program push/PR/manual CI workflow runs the default/domain operator and worker runners and uploads complete archives and failure evidence. Hosted acceptance remains separate. No SIGKILL, leaf, Postgres/domain, full matrices, million physical drain or actual24h acceptance is inferred.
