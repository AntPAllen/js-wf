# Indexed native journal and worker renewal recovery — 2026-10-10

`journal.NativeGraphConfig.OwnerScopeIndex` explicitly selects a newly
provisioned indexed authority. `NativeGraphStreamConfigs` returns its distinct
subject configuration, and `OpenNativeGraphStore` opens the indexed metadata and
object-port wrapper while preserving its owner-discovery interface. Zero/false
keeps the legacy graph configuration and full-discovery behavior. No existing
stream is changed, and no public continuation admission is opened.

Two journal mode controls provision either legacy or indexed stores, open the
matching mode, reject the other mode, and verify that both stream configurations
and physical high-water sequences remain unchanged by rejection. The existing
public-admission control also passes. These admission reads never provision or
convert a store.

## Worker evidence

Native R1/R3 with JetStream domain STOREDWORKER exercise three fresh workers and
fresh graph/descriptor adapters across a renewal cut. Actual invocation, lease,
graph, object, file-backed descriptor KV and dispatch ports are used. The first
worker completes two staged records, advances the controlled journal clock by
41 seconds, saves its requested renewal expiry, renews one scope batch and is
cancelled. Independent inspection confirms the saved prefix and original source
head, with no remaining reader. A fresh worker resumes the pending renewal from
storage, completes staging, advances authority time a further 20 seconds across
the original intent expiry, independently verifies the target and dispatches.
The third worker enters the next stage exactly once. Descriptors are removed.

SDK request traces count owner-filtered subject censuses only while execution
is active. Both renewal attempts must use owner-filtered discovery, with no full
namespace census. Replacing the indexed wrapper with a bare native port makes
both R1/R3 cases fail this assertion, even though the small fixture can otherwise
finish via the full scan. The exact bypass and terminal failures are retained.
Source is restored before the accepted race selection. Legacy R1/R3 stored
maintenance controls are rerun in the same package selection.

The worker fixture retains its original two-minute caller watchdog, real request
and batch contexts, and real native lease authority. Only the journal intent/
reader clock is controlled. This is a fresh worker object per delivery; there
is no OS process kill or server restart in this fixture. Same-store native peer
restart coverage belongs to the preceding index component evidence. The initial
pre-trace run is preserved and excluded from the final traced qualification.

## Remaining work

Large owned-grant enumeration and pagination remain unqualified. Native discovery
currently witnesses every marker, so setup cost itself grows with the owned
grant count inside the worker's three-second renewal-begin context. Scope-index
registration also adds metadata requests per blob mutation. Qualify these costs
and design a bounded renewal setup before another multi-hour 100,000-entry run.
The index removes unrelated-owner work only when explicitly configured; it does
not provide constant work for a publication's own scopes. Existing namespaces
require a proven complete migration and stay on full discovery. The original
full scale, fault, retention, majority, soak, admission, collection and rollout
gates remain open.

```sh
go test -race ./worker -run '^(TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery|TestNativeGraphStoredMaintenanceFreshWorkerRecovery)$' -count=1 -v
go test -race ./journal -run '^(TestNativeGraphOwnerIndexModeAdmission|TestNativeGraphJournalPublicAdmission)$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-indexed-worker-renewal-2026-10-10/review.py
```
