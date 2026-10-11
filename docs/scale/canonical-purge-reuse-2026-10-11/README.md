# Canonical Phase 7 purge/reuse scenario

The existing legacy 10k-completed / 10k-live / 1k-reused-ID scenario now has an
explicit canonical native R3 variant. It provisions protected indexed graph
stores, uses canonical Start/Signal/worker journals and `PurgeGraph`, and retains
all generation, epoch, stale-signal, fresh-signal, result and queue-drain checks.
The default population and 20-minute parent / 22-minute test watchdog are unchanged.
Count overrides are diagnostics, never full-scale acceptance. PostgreSQL remains
a separate legacy projection scenario. Public continuation and online GC stay off.

One pending purge waits until the first reused Start has succeeded. This guarantees
the requested cohort overlap without relying on goroutine completion order. It
proves reuse while the purge cohort remains unfinished, not simultaneous execution
of two individual server publications. Suspended live invocations are the default;
`WF_PURGE_REUSE_ACTIVE=1` additionally requires every live handler to be executing
before purge and still active afterwards.

Polling graph suspension now uses cursor inspection rather than repeated pinned
full reads, which mutate reader metadata and fence the append being observed.
All six workers join before the independent raw namespace audit. The fixture
requires eleven current terminal generations and nine generation-bound tombstones
at count 10, plus an empty legacy journal and zero reader pins. At full count those
requirements become 11,000 terminals / 9,000 tombstones / 66,000 current entries.

## Auditor correction and compatibility

The native diagnostic discovered that the raw auditor treated purge tombstones
as terminal Outcome envelopes. It now independently admits exact-generation
markers only for retired, purge-fenced roots without invocation sources. Typed
marker/generation/times must be valid, expiry must follow purge time, and unknown,
aliased, duplicated or contradictory fields fail. Forty-eight JSON/protobuf
controls cover these cases. Retained terminal Outcome projections continue to be
reported as projection-only evidence; tombstones have their own report counter.
Neither counter proves erased result ownership, wall-clock expiry or purge history.
The complete native fixture additionally requires actual tombstones for every
purged, unreused generation.

## Closed diagnostics

| Case | Actual exit | Package duration |
| --- | --- | --- |
| Canonical R3 normal, 10/10/1 | 0 | 12.294 s |
| Canonical R3 race, 10/10/1 | 0 | 71.360 s |
| Legacy normal compatibility, 10/10/1 | 0 | 5.722 s |
| All raw graph audits, normal | 0 | 1.907 s |
| All raw graph audits, race | 0 | 17.515 s |

Both raw audit modes pass 1,875 named tests. Independent `review.py` binds 1231
Go/YAML/module inputs, five final actual executable hashes/exits, complete closed
native censuses (14,938,090 and 23,177,787 bytes) and both final CI reducers.
Binaries and stores remain at `/home/exedev/js-wf-canonical-purge-reuse-20261011`.
No full 10k or active-handler acceptance is claimed from these diagnostics.
The manual CI profile preserves count 10,000; regular CI is explicitly diagnostic.
The hosted workflow was not run here.

Earlier outcomes remain preserved: the first polling run was intentionally stopped
with SIGTERM (not a watchdog or test failure); the first completed run rejected a
valid tombstone; the broader raw audit initially rejected four retained-Outcome
compatibility controls; the legacy diagnostic once missed cohort overlap. The
read-only public NATS monitor is diagnostic, not a server-cause proof. Early
component compile/unit command receipts were recorded after execution without
captured executables. No native server cause is asserted.

## Full campaign preparation

`full-supervisor.py` requires a unique root and copies every source input directly
from the specified committed revision. It copies the already-qualified normal
binary and runs the original 10k/10k/1k population with **active handlers**, retaining
actual PID/birth, command, environment, exit, log, source hashes and closed native
census. `full-review.py` refuses live/incomplete state and requires every full-count
result. This proves functional purge/reuse only; fault matrices, storage power
loss and physical graph collection remain separate gates.

The Go build cache was cleared through `go clean -cache` to make room; the receipt
records free space and unchanged retained binary hashes. No proof store or frozen
checkout was deleted. Full campaign launch and outcome are recorded separately.
