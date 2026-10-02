# Delivered-source graph: 119-workload 100k acceptance

[Run 37039245566](https://github.com/AntPAllen/js-wf/actions/runs/37039245566)
finishes successfully at exact `7c5fb3ea468836ee95fd5b3e48318b1e5467b292`.
Independent inspection accepts the actual package pass in 7,461.495 seconds,
163 top-level passes, two documented trace-only skips, 265 source pins and
exact seeds 1..100000 for each of 119 workloads: 11,900,000 actual bodies.
Aggregate coverage is 12,100,034 schedules, 179,346,691 scheduler choices and
2,669,163,306 transport events.

The compiled test inventory and seeded-loop AST inventory were regenerated
from the extracted campaign Git revision and match uploaded evidence. Regression
paths match Git. The complete suite report regenerates byte for byte from the
original Go JSON events using that revision's checker. Terminal job metadata,
complete campaign log and all source hashes are retained.

All 856 originals, including extracted Go/module/pin sources, uploaded reports,
events, inventories and independent checks, are archived losslessly. Every
member's SHA256 was verified on readback before atomic publication. See
`manifest.json`; originals remain in `/tmp/js-wf-tier1-119-100k-37039245566`.

This accepts the graph including native-delete reply recovery and delivered
native-source retention. It predates the held-lease continuation takeover
workload and cannot qualify the current 120-workload release gate. Full fault
matrices, original million-timer persistence/drain and full-runtime 24-hour soak
remain open.
