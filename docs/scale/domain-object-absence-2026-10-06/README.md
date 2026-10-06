# Forced object absence on real JetStream domain

Prepared focused tests extend the existing result-read and compacted-snapshot controls to real R3 servers in `WFRESULT`. Actual connection and AccountInfo domain admission is checked on all three peers. Traced clients use the public NewWithDomain constructor; expected administrative leader routes include the actual domain.

Only weak metadata absence/old-manifest responses are injected. Payload, deletion, corrupt metadata and administrative confirmation use real file-backed servers. Existing deadline/cancellation and hash/gap rejection checks stay strict. The default-domain controls will run alongside the new domain cases. Native qualification is pending; this is not natural follower-lag or combined retirement/server-fault evidence.
