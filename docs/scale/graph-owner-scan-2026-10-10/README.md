# Incremental complete owner discovery — 2026-10-10

Compaction renewal now prefers optional `OwnerScopeScanPort`. Setup opens a
fresh cursor instead of fetching the full owner key list. Each Advance fetches
at most its scope budget, rejects invalid/duplicate identities before scope I/O,
validates registrations and grants, and rechecks original source/time. Discovery
or scope failure is sticky; no partial plan escapes. Fresh recovery scans again
and reconciles the exact requested expiry, rather than trusting a saved prefix.
Static owner and legacy namespace discovery remain available for older ports.

## Native completeness mechanism

1. Every native blob mutation requires acknowledged permanent index registration
   first. Staging under the same owner is frozen during renewal.
2. Setup reaffirms a reserved, owner-specific absent-scope registration. Its
   acknowledged physical sequence is the scan boundary. It is not a grant and
   cannot revive one. Each owner has a distinct reserved key to avoid an unrelated
   publication contending on the same absent blob witness subject.
3. One filtered STREAM.INFO pagination request asks beyond the supported owner
   cardinality and obtains its `total` without returning keys. The response must
   name the correct stream, have a sequence at least the boundary, contain no
   key map, and fit 64 KiB. Errors and unsupported counts fail. API routing honors
   domain/custom prefixes and request tracing. A caller without a deadline gets
   the SDK-configured default timeout.
4. The cursor obtains the next owner marker by physical sequence, within the
   fixed boundary, and validates canonical identity bytes. At the boundary it
   must find its reserved marker and exactly the expected count. The protocol
   separately rejects duplicates across batches. Moving an undiscovered marker
   above the boundary loses a counted entry and fails; moving an already read
   marker is safe. Replacing the boundary also fails.
5. Final grant/content verification and the original-head CAS remain independent.

The public request/response shape is documented in the
[NATS API reference](https://docs.nats.io/reference/reference-protocols/nats_api_reference)
and [v2.15.0 server implementation](https://github.com/nats-io/nats-server/blob/v2.15.0/server/jetstream_api.go).
No new stream configuration, automatic import, namespace migration, deadline or
TTL change is introduced. Existing indexed namespaces retain complete registration
from earlier adapters; their full-list readers can cause conservative churn
failures if used concurrently with this scan, rather than false completeness.

Setup has bounded request count and client response size. Server-side filtered
count calculation still grows with owner cardinality. The client retains seen
identities as it scans to reject duplicates; total memory is not constant. The
scope budget bounds discovery/validation cardinality, not every internal retry
or all server work. Errors start a new cursor; scan state is not serialized as a
certificate in the descriptor.

## Evidence

Twelve model controls cover normal/fresh-cursor reconciliation, unknown/nil begin,
unknown batches, duplicates within/across batches, oversized/no-progress/invalid
results, source changes and expiry. These plus seven witness, eleven owner and
seventeen prior renewal safety controls pass restored race 5.065 seconds.

Five native R1 controls exercise healthy witness churn, undiscovered-marker
churn, boundary churn, noncanonical marker bytes and lost reads. Both native
R1/R3 restart controls retain orphan/reservation reconciliation and final
independent compaction. They now discover ten registrations, including the extra
boundary reservation. Removing count equality fails the native omission control;
removing duplicate rejection fails two model controls. Source is restored after
each bypass. A development duplicate fixture that panicked under bypass, and
initial native assertions predating the extra reservation, remain excluded from
accepted evidence.

The native R1 large-index fixture acknowledges 100,001 reservations plus six
real grants. With the boundary reservation, setup counts 100,008 registrations
in 275.172398 ms under the unchanged three-second context, fetches no key list,
and performs one count request and one boundary witness. Its next budget-two
batch discovers/validates only two scopes. The restored native selection passes
race 22.591 seconds. The older full-list pagination fixture explicitly hides the
new interface so it can continue measuring that separate fallback.

Native domain R1/R3 worker renewal recovery passes race 20.014 seconds after the
bypasses, preserving saved requested expiry, exact source, fresh verification,
one next-stage call, descriptor deletion and owner-filtered discovery. All 853
common pins pass 7.625 seconds. Frozen sources, exact bypass bytes, commands and
terminal logs are retained. The reviewer checks this scope only.

## Remaining qualification

The large fixture contains reservations and six grants, not 100,000 journal
entries. It advances only two scopes. Total renewal work must still finish
before original expiry, and each real grant needs metadata reads/CAS. Qualify
large owned-grant latency and liveness before another multi-hour full-entry
padding run. The count query's larger-cardinality server cost, accounts/security,
fault/storage/VM loss, original full seed campaigns, majority, soak, retention
and rollout gates remain open. Admission and collection stay closed.

```sh
go test -race ./internal/graphpublication -run '^(TestGraphCompactionOwnerScanCompletenessAndBounds|TestGraphCompactionOwnerWitnessScopeBudget|TestGraphCompactionOwnerScopeDiscovery|TestGraphCompactionIntentRenewalScopesAndFences)$' -count=1 -v
go test -race ./internal/graphpublication -run '^(TestNativeGraphOwnerScanSequenceChurnCompleteness|TestNativeGraphOwnerScopeIndexRecovery|TestNativeGraphOwnerScopeIncrementalSetupAt100000)$' -count=1 -v
go test -race ./worker -run '^TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-owner-scan-2026-10-10/review.py
```
