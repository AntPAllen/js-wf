# Seeded owned-grant renewal cost — 2026-10-10

Incremental discovery removes eager client census work, but renewal still costs
one discovery, marker validation, grant read/CAS, and several original-head
checks per owned grant. This experiment isolates that remaining liveness limit
without NATS, faults, sleeps, or concurrent mutation. Production is unchanged.

The real 12-record compaction fixture contributes 20 grants. An authoritative
owner index registers another 1,000, 10,000, or 100,000 valid uploading grants
through CAS before measurement. These abandoned grants are absent from the
final forests and must remain in renewal's authority set. A fresh incremental
cursor discovers at most 128 identities per Advance; no setup key list or
per-key witnessing is allowed.

The seeded transport charges each modeled operation a uniform 0.5–1.5 times
the assumed latency. Renewal begins with 20 seconds remaining on a 60-second
intent and requests expiry at 120 seconds. Seeds 2, 5, and 42 exercise 1,020 and
10,020 scopes; seed 42 additionally exercises 100,020 scopes. There are 11 cases.

| Total grants | Assumed operation latency | Outcome |
| --- | --- | --- |
| 1,020 | 1 ms | All renewed in about 7.15 seconds |
| 10,020 | 1 ms | Original expiry reached after about 2,830–2,835 renewals |
| 10,020 | 10 µs | All renewed in about 0.70 seconds |
| 100,020 | 1 ms | Expiry after 2,830 renewals; 97,190 retain old expiry |
| 100,020 | 10 µs | All renewed in 7.019511954 seconds |

Race PASS (64.208 seconds) means all expected successes and counterexamples were
reproduced. It does not mean the 1 ms large cases meet a liveness gate. Every
case checks the scope budget, grant accounting, and unchanged source root.
Failure returns no plan, becomes sticky without further I/O, and a fresh scan
cannot revive the expired old plan. Existing 17 renewal fence controls also
pass (0.151 seconds).

These costs are assumptions, not measured native latency. One modeled call can
require several native wire requests, so the model is optimistic. The fixture
has 100,000 additional authority grants, not 100,000 journal entries. Native
total-renewal and actual full-entry acceptance remain open. No server-side
cause is inferred for older failures.

## Reproduce

```sh
go test -race ./internal/graphpublication -run '^TestGraphCompactionOwnedRenewalCost$' -count=1 -v
go test ./internal/graphpublication -run '^TestGraphCompactionIntentRenewalScopesAndFences$' -count=1 -v
python3 docs/scale/graph-owned-renewal-cost-2026-10-10/review.py
```

Logs and compressed package sources preserve the tested experiment. The
reviewer checks source bytes, all 11 traces, expected case membership, complete
versus expired outcomes, grant counts, and the 17 existing safety controls.

## Next requirement

Establish a renewal strategy and explicit supported lifetime/cardinality budget
that can cover all owned grants before their original expiry. Increasing the
handoff deadline or speeding setup alone cannot fix this per-grant limit.
Evaluate a publication-level authority or a separately configured compaction
lifetime against this model before another multi-hour full-entry run. Preserve
abandoned and uncertain uploads, exact original-head fencing, unknown-outcome
failure, independent final verification, and refusal to revive expired grants.
Any larger configured lifetime needs measured native total cost and recovery
coverage; it cannot itself count as scale acceptance.
