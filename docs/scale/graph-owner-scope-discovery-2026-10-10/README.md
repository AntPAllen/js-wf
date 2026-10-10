# Complete owner-scoped renewal discovery — 2026-10-10

The renewal primitive now accepts an optional `OwnerScopePort`. Its discovery
contract includes all permanent scopes for the publication token: ready,
uploading, closed, abandoned, and uncertain writes. Storage must register a
scope durably before it can be created. Fresh adapters share that registry.
Reservations without a blob record are allowed. Final forests and client-saved
descriptors cannot establish completeness.

Renewal uses this discovery once, copies/sorts the returned keys, rejects
duplicates, and independently validates every record. Foreign nonempty records
fail. Unknown enumeration returns an error without a full-scan fallback or
mutation. Original-head checks, expiry rejection, scope budgets, sticky batch
errors and final verification remain unchanged. Ports without the optional
interface retain their previous full namespace scan.

## Evidence and scope

The in-memory authoritative index registers before CAS and survives fresh
adapter construction by retaining its storage registry. Eleven controls cover
normal operation, 100,000 unrelated closed scopes, abandoned uploading grants,
unknown committed creates, absent reservations after rejected writes, fresh
adapters, unknown enumeration, duplicate keys, foreign records, invalid keys,
and source mutation during discovery. The large foreign namespace is populated
through the indexed port, rather than supplied as a client hint.

With seed 42 and approximately 1 ms per successful metadata operation, both
54-scope and 100,054-scope namespaces use 20 blob reads/20 updates and finish in
95.310 ms of modeled time. Orphan/unknown-create cases examine and renew 21
grants. Reservations are checked without inventing a grant. Foreign scopes
retain their original revision, closed phase and empty intents. No source head
is published by renewal. These are assumed costs, not native latency numbers.

Forcing the protocol to use the old full scan makes the large-namespace control
fail. The exact bypass source and terminal failure are retained. After restoring
source, all eleven controls and the existing seventeen renewal safety controls
pass under race (3.798 seconds). The common regression corpus is retained in
`pinned.log`. Source hashes and compressed input bytes bind this experiment;
the reviewer checks the terminal logs, cases, bypass and corpus count.

```sh
go test -race ./internal/graphpublication -run '^(TestGraphCompactionOwnerScopeDiscovery|TestGraphCompactionIntentRenewalScopesAndFences)$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-owner-scope-discovery-2026-10-10/review.py
```

## Native work remaining

NativeAuthority/NativePort do not implement this interface yet. Their existing
subject layout hashes owner and payload together, so owner filtering cannot
discover existing scopes. No partial index is enabled on existing streams.
A new native storage design must establish complete registration before every
scope write, preserve it through unknown outcomes and restarts, and prevent old
writers from bypassing it. Existing namespaces require a proven migration or
must remain on full discovery. Native R1/R3, fault/restart and full owned-grant
cost controls are required before claiming scale liveness. This primitive does
not clear actual 100,000-entry acceptance, admission, collection or rollout.
