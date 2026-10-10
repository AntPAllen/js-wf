# Seeded renewal namespace latency counterexample — 2026-10-10

This experiment isolates an algorithmic liveness limit in the current renewal
protocol. It does not explain the historical native cluster failures. No NATS
server, wall-clock sleep, RPC failure, collection, or concurrent mutation is
required. Production behavior is unchanged.

`TestGraphCompactionIntentRenewalNamespaceLatency` wraps the existing in-memory
authority port with a seeded request cost. Each root read, blob read, blob CAS,
and namespace census advances authority time by a uniform 0.5–1.5 times its
configured latency. It starts with 20 seconds left on a 60-second intent, uses
128-scope batches, and stages a real 12-record source with 20 owned grants.

For seeds 2, 5, and 42, the small namespace succeeds at 1 ms/request. Adding
100,000 valid permanent closed scopes owned by an unrelated token makes renewal
expire before completing its census processing. At 10 µs/request the same large
namespace succeeds. The failure is the expected counterexample in this test:
PASS means the test reproduced the limitation while preserving safety; it does
not qualify renewal at scale.

Each case checks that renewal publishes no source head, never changes foreign
scopes, stays within its scope budget, and only extends owned expiries to the
requested value. Failure returns no plan and remains sticky without additional
transport requests. Successful cases renew all owned grants. Existing renewal
controls separately retain expired, abandoned upload, unknown CAS, source race,
reader, collector, and ownership cases.

The model is deliberately optimistic: one charge for enumeration, no per-byte
network/serialization cost, and one charge for each port operation. Native CAS
may require more than one request. These are assumed costs, not measured NATS
latencies. There are 100,000 unrelated authority scopes, not 100,000 SDK journal
entries. The original full-entry acceptance gate remains open.

## Reproduce

```sh
go test -race ./internal/graphpublication -run '^TestGraphCompactionIntentRenewalNamespaceLatency$' -count=1 -v
go test ./internal/graphpublication -run '^TestGraphCompactionIntentRenewalScopesAndFences$' -count=1 -v
python3 docs/scale/graph-renewal-namespace-latency-2026-10-10/review.py
```

`race.log` contains the nine cost traces and terminal package status.
`fences.log` contains the existing 17 safety controls. `sources.json` and compressed
inputs preserve the tested source. The reviewer checks those bytes and terminal
logs; its scope is this experiment, not full-plan acceptance.

## Next implementation requirement

Renewal work must stop growing with unrelated namespace history. A scoped
enumeration or publication lease design must retain complete authority over
owned ready/uploading scopes, including abandoned branches and uncertain
uploads absent from final forests. A reachable-forest-only list is insufficient.
Fresh recovery must establish completeness without trusting a saved client
descriptor as authority. Preserve exact original-head fencing, unknown-outcome
failure, no expired grant revival, and independent final verification.

After removing unrelated-scope cost, qualify cost for a publication's own large
grant set before retrying the multi-hour native 100,000-entry fixture. Longer
handoff contexts and larger batches alone do not solve the namespace dependency.
