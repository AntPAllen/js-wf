# Canonical timer origins and acknowledged shifted hints

The worker optionally records `timer_native_hint` with the physical timestamp,
canonical lower/upper bounds, exact deadline and translated schedule time,
publication classification and durable request index. Failed publication is
uncertain; duplicate acknowledgment does not prove a new retained hint.
SDK logical timer steps differ from journal indices because suspensions and
attempts add journal records. Diagnostic mapping preserves checkpoint offsets
and actual request indices without changing timer IDs. Mapping is allocated
only when native domain tracing is enabled.

Admission supports both legacy and canonical requests. Canonical creation must
match `ClockUpper + duration`, overlap the unshifted controller operation bracket,
and have exactly one valid origin. A separate fresh acknowledged native hint
must match that deadline/domain/request and the actual shifted-source timestamp,
with exact `ServerTime + max(deadline - ClockLower, 0)` translation. Unknown,
duplicate, late, wrong-owner, unshifted or ambiguous hints are not admitted.
The cut still removes the source before creation-request-start plus duration,
refreshes the retained suspended tail and corroborates the final prefix.
Canonical upper-bound padding does not extend this admission boundary.

Controller latency audit uses canonical creation observations while keeping the
original early-completion and per-type terminal/progress targets. The Python
cut reviewer validates the two proofs; the role reviewer reports legacy lookups
and fresh native hints separately. R5 fixture writers remain legacy until the
shared provider and observations are enabled together.

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./worker -count=1
GOMEMLIMIT=512MiB GOMAXPROCS=2 go test -race -p=1 ./integration -run '^TestMatrix(CanonicalTimerAdmission|PendingClockTimer|Controller)' -count=1 -v
GOMEMLIMIT=512MiB GOMAXPROCS=2 WF_CONTROLLER_AUDIT_OUT=/tmp/canonical-controller go test -race -p=1 ./integration -run '^TestMatrixControllerLatencyAuditCanonicalTimerWorkflow$' -count=1 -v
python3 -m unittest discover -s scripts -p 'test_tier3*.py'
```

Final worker race passes31.431s. Combined admission/controller tests pass
package19.765s, including26 canonical rejection/acceptance cases across both
clock directions and unchanged legacy controls. After adding exact journal-index
assertions, the final actual canonical R3 workflow passes7.54s / package8.570s
with eight fresh hints matched to their retained requests. Its ideal UTC provider
uses±20ms bounds; independent sampling is covered separately. All39 Tier3 Python
test methods pass, including canonical-cut and role-profile controls. Complete
logs/source hashes plus all six actual controller artifacts are retained and
uncompressed hashes verified.

This verifies admission/audit and production trace correlation; it does not prove
a source was killed in the canonical workflow or clear native clock recovery.
Ahead60.27s, incomplete behind sustained, final-source comprehensive100k, full
fault combinations and five-node24-hour release gates remain open.
