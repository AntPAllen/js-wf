# Held-lease entry observation without additional broker reads

Executed clean `ad92227`. `AcquireWithHeldObserver` reports the KV entry already
read during acquisition, preserving Acquire decisions, request order, exact
ErrHeld identity/error text, TTL12s/AckWait13s and held retry5s. Worker dispatch
attaches this optional metadata only to lease_held events, when an observer is
installed. Other event JSON omits the new field. No extra broker requests.

Metadata includes key, revision, decoded worker/epoch, KV/server creation time,
client observation time, and reason. Malformed values are not attributed to an
owner; races without a read explicitly report entry_observed=false. Reclaim CAS
conflicts report the prior read, not an invented current winner. Client/server
clock comparison is diagnostic, not authority to reclaim an initialized lease.

## Verification

- Six lease controls: initialized, malformed, missing-entry/create race,
  stale-orphan reclaim conflict, unavailable, successful acquisition. Observed
  and ordinary Acquire return the same result and broker call sequence.
- Worker controls with/without observer preserve one Create/one Get and5sNAK;
  exact held metadata reaches only the held event. Race controls passed.
- Actual three-node file-backed KV, production12sTTL: nativePASS2.66s, revision
  2→3/creation timestamp advances on renewal while fencing epoch stays1.
  Two failed acquisitions total twoCreates/twoGets/zeroUpdates/zeroDeletes.
- Held-terminal model1000race seeds and all392 regression pins replay exactly.
  Observation changes do not add seeded choices or transport mutations.

Actual captured lease/worker/sim test executables and full build fields bind to
1,049 Git source/pin inputs, unchanged before/after. Native servers are real NATS
instances embedded in the captured lease.test process; three unique server IDs
and R3/file/12s stream configuration retained. No separate server process hash
claim. No latency cause, full worker-kill/matrix/24h qualification.

The preliminary uncommitted native fixture timed out at30.04s during full-runtime
provisioning before observation. Its failed test source/report are retained, but
its temporary stores and actual executable were not captured. It is unqualified
and its provisioning cause remains unknown. The accepted diagnostic provisions
only its required lease bucket with bounded readiness calls inside the unchanged
30s deadline; this does not qualify full-runtime provisioning.

All 1,122 proof members / 52,493,939 bytes / 3 parts independently read/hash-verified, SHA256 `00d8e4dd3aa4d3d29b1d6bfc4194fdde9466d3bfbf7e4cfe6a015d9f203b7c3c`. See [review](independent-review.json), [actual native observations](held-observations.json), [archive verification](archive-verification.json).
