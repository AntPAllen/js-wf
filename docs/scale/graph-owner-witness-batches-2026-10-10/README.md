# Owner marker witnessing moves into renewal batches — 2026-10-10

Native owner discovery now returns its complete owner-filtered key census without
performing a marker witness for each key. The optional `OwnerScopeWitnessPort`
separates registration validation from discovery. Compaction renewal calls that
validation once per examined scope inside `Advance(maxScopes)`, then rechecks
time and the exact source before reading the blob record. Unknown/corrupt,
expired or source-changing witnesses stop the batch; no target plan escapes.
The failure is sticky. Native registration before blob mutation still applies.
Independent scope authority and final content verification remain required.

This bounds marker validation by the existing scope budget. It does **not** bound
census serialization, owned-key memory, or total renewal duration. No production
timeout, intent TTL, worker admission or collection setting is changed.

## Evidence

Seven seeded request-cost controls cover healthy renewal, setup with 100,000
absent reservations, unknown witnessing in a later batch, expiration during
witnessing, source mutation, malformed markers and cancellation. Setup performs
zero marker witnesses. A budget-two batch validates at most two markers.
Later failure confirms three scopes before the fourth marker fails, and does
no blob I/O for that marker. Skipping witness validation fails six controls.
After exact source restoration, these seven controls plus eleven prior owner
discovery cases and seventeen prior renewal safety cases pass race 5.327 seconds.

Native R1/R3 index recovery passes race 14.626 seconds. Its lost-marker control
now fails an Advance batch before blob I/O, rather than census setup. Existing
all-peer same-store restart, orphan/reservation renewal and final compaction
verification remain exercised. Native domain indexed worker recovery also
passes race 20.895 seconds, retaining saved pending renewal and owner-filtered
discovery. The common 853-pin corpus passes 8.083 seconds.

## Native pagination and an unresolved timing gate

A dedicated R1 fixture adds 100,001 canonical permanent index reservations,
inspects every asynchronous publication acknowledgment in bounded groups,
and retains six real compaction grants. Its owner census discovers all 100,007
keys over two SDK/server subject pages, including the page at offset 100,000.
Setup performs no marker witnesses; the next budget-two batch validates two
markers. An actual grant in the batch can separately reaffirm its registration
during CAS, so transport witness counts are not scope cardinality.

The final run passes the three-second setup context in 2.926577205 seconds and
the package passes race 16.648 seconds. Earlier runs fail the same deadline:
`failed-three-second-census.log` records 3.000386520 seconds; the pre-counter
diagnostic records a 3.158464950-second failure followed by successful pagination
under a separate 15-second diagnostic context (3.283506098 seconds). Those earlier
fixture versions are retained and do not qualify the final source. Production
code governing discovery is unchanged between these observations. The longer
diagnostic does not clear the original three-second failure.

The fixture accepts deadline failure as a reproduced limitation, then uses a
separate bounded diagnostic to inspect pagination. Consequently test PASS is
not stable large-census timing acceptance. These are index reservations plus
six grants, **not 100,000 journal entries**, and only two scopes are renewed in
the large fixture. Complete large renewal before original expiry is unqualified.
The server stores are temporary and cleaned after terminal test completion.

## Next work

Replace whole-key census setup with complete bounded discovery. Its proof must
survive marker witness sequence churn, unknown writes, all owned/abandoned scopes,
fresh recovery and source fencing; a partial snapshot must never produce a plan.
Then qualify total renewal cost for large owned grant sets before spending another
multi-hour actual 100,000-entry run. Existing namespaces retain full discovery;
the broader full scale, fault, retention, majority, soak and rollout gates remain
open. No deadline was enlarged to accept the large census.

```sh
go test -race ./internal/graphpublication -run '^(TestGraphCompactionOwnerWitnessScopeBudget|TestGraphCompactionOwnerScopeDiscovery|TestGraphCompactionIntentRenewalScopesAndFences)$' -count=1 -v
go test -race ./internal/graphpublication -run '^TestNativeGraphOwnerScopeIndexRecovery$' -count=1 -v
go test -race ./internal/graphpublication -run '^TestNativeGraphOwnerScopeCensusPaginationAndDeferredWitnesses$' -count=1 -v
go test -race ./worker -run '^TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-owner-witness-batches-2026-10-10/review.py
```
