# Accepted complete normal 100,000-seed suite

Run [37146526818](https://github.com/AntPAllen/js-wf/actions/runs/37146526818),
job `111271509376`, succeeds at exact
`9ecc37c74979132a8a2bae5877eb44bc175cbf89`. Artifact `11286592374`
(`tier1-extended-suite-evidence`, 12,606,733 bytes) retains the actual executable
and original execution evidence.

Independent review accepts all 121 workloads at seeds 1–100,000: 12,100,000
completed seeded bodies, 175 passing top-level tests, 391 passing pinned
regressions and exactly the two documented trace-only helper skips. Package
elapsed is 12,288.631 seconds; producer wall time is 12,288.98 seconds.
Aggregate fixed/repeated/generated coverage totals 12,301,847 schedules,
179,755,797 choices and 2,694,460,058 transport events.

All 1,140 before/after source hashes equal exact Git bytes. The regression
inventory, actual compiled test inventory and source-derived seed inventory
match; the complete suite report regenerates identically from raw Go events.
Retained normal executable SHA256:
`6901c1b08d5ecf4b6c23f4f906e031636dd517041c1469221c90384975c13738`.
Its bytes/build settings, actual build/execution commands and contexts verify.
Download stripped the execute bit; restoring it changed no executable bytes.

A separate 686-file equivalence ledger verifies identical tracked non-test
Go/module inputs, complete simulator sources/pins, Tier1 producer/suite checker
and both Tier1 workflows against reference
`9c5fce320072ef9b6b1fc5e5d6e6822549d6b0ab`. Later integration-only fixtures
and unrelated verifier/documentation edits are outside this claim. Thus this
qualifies the complete current runtime/simulator graph's normal100k gate;
the [separate accepted full race1k suite](../hosted-race/) covers the same tested
source. It does not certify unexecuted real integration fixtures or the full
matrix, five-VM, 24h or original physical million-timer drain requirements.

The 23-member archive (12,508,231 bytes) retains the downloaded originals,
completed API observations/job log, actual independent reviewer, verdict,
source-equivalence ledger and summary. Every member reopens and SHA-verifies
before atomic rename. This simulation run has no physical broker stores.
