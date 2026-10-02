# Remaining four same-store mutation smoke pairs

All four **35-second** pairs execute at `026d574c45627438d18eee3110b466dca1a85062`.
The new campaign verifier independently matches the complete recorded source
inventory against Git, verifies the exact production and negative-control
overlays, requires actual named baseline passes and specific mutant failures,
recomputes p99 from original timestamps/samples, checks fault identity/chronology
and original-cohort preservation, and reproduces each original phase report.

| Category | Original completed cohort: baseline / mutant | Observed detection |
| --- | --- | --- |
| Purge | 168 /196 | Intact purge resumes and reuses a fresh generation/index0..3; invocation-first mutant loses its source and a real retry returns ErrNotFound |
| Leases | 224 /196 | Intact lease excludes a rival; private-worker-key mutant admits the rival while the replacement holds the original invocation |
| Determinism | 224 /196 | Intact replay rejects the renamed step; mutant executes it once and writes Completed/result42 |
| Enqueue | 196 /196 | Intact64 enqueues retain one ID-tagged dispatch; mutant retains64 distinct physical records without message IDs |

Each phase retains the original completed mixed cohort while the guard challenge
runs on the same cluster and stores after an additional verified journal-leader
SIGKILL. The original audits establish valid state and unchanged counts, not
byte identity of every retained record. Raw enqueue receipts, purge generation
records and both actual negative controls are independently checked. Real NATS
receipts encode an absent header as `null`; the verifier accepts that representation
and rejects any hidden message ID on the mutant records.

The four pairs complement the earlier [CAS/start smoke and race evidence](../local-smoke/).
They do not constitute six accepted ten-minute release pairs. Hosted smoke
campaign37018302340 is still running at this source. Ten-minute acceptance,
the200-seed full matrix and24-hour full-matrix soak remain open.

`originals.tar.gz` losslessly retains94 original files, phase reports, independent
checks and the final verifier/tests. Every member was read back and SHA256 matched
to its original bytes; see `sha256.json`. `independent-check.json` records the
regenerated checks. Nine parser/campaign tests pass, including source and job
identity, actual execution, raw CAS/start proofs, null-header enqueue records,
shortened scope, missing artifacts and corrupted latency/cohort rejection.

Commands, repeated for `purge`, `leases`, `determinism` and `enqueue`:

```sh
GOMEMLIMIT=512MiB GOMAXPROCS=2 python3 scripts/check-invariant-mutations.py \
  --mixed-purge --sustained 35s --output /tmp/js-wf-sustained-purge-smoke-20261002
python3 -m unittest discover -s scripts -p 'test_sustained_mutation*.py'
```

