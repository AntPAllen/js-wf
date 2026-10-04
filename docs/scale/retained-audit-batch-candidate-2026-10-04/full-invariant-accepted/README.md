# Full invariant audits with opt-in batched reads

Executed source `9a8894639dc51eafa75e9e9bad75b1d24d24549e`, retained actual race
SDK, three real in-process NATS nodes with file stores. Default Check APIs retain
point reads; new opt-in full/cohort APIs use batching and the same invariant body.
Audit20s/60s limits are unchanged.

## Native results

-3000 retained invocation fixtures /36000 journal entries /3000 terminal records:
  both readers return the exact expected full report. Point audit12.429541066s;
  batched audit2.606781530s. Both complete under their original20s deadlines.
- Compacted prefix reconstruction gives the same two-invocation/eight-entry report.
  Later malformed journals are excluded before decoding during a captured-cohort
  audit; the final full audit rejects them.
- Corruption after a successful audit is read again: changed or missing terminal
  state and a poisoned snapshot object fail with the same reports/errors in both
  readers. Restoring the original object restores success; no earlier audit is
  cached as proof.
- Full audits reject orphan journals and duplicate invocations. Cohort checks
  exclude later/orphan records and still require a final full audit.
- Shared-epoch writers, index gaps, descending epochs, successful terminals with
  unresolved requests and completions without requests yield matching reports
  and errors. Temporary consumers are cleaned up in the compacted/cohort case.

These are retained-state fixtures, not workflow execution or latency-model results.
Whole actual run47.829944218s. Raw named outcomes and comparisons are retained.

## Complete independent proof

All2889 captured inputs /59 Git-local files, pre/post hashes, actual runner bytes
against Git, actual SDK SHA and every build-info field verify. Actual SDK SHA:
`ead834a7c06eff2e09900a478b7217bf13904bbe01bcced939a94ef0bfc3454d`.

Complete3977-member /109975564-uncompressed-byte proof retains actual SDK,
selected local/module/toolchain inputs, native source/data/raw reports and original
store bytes. Archive35636496 bytes in two ordered parts. `manifest.json` records
all member hashes/sizes, part hashes and concatenated SHA. Member readback,
unchanged originals, part readback and concatenated hash all verify. This is a
selected input superset, not exhaustive assembly/embed/generated/hermetic proof.

Concatenate parts in numeric order, verify hashes and extract into a fresh directory:

```bash
python3 review.py --root /path/to/extracted/producer --repo /path/to/js-wf --output /tmp/full-audit-review.json
```

Original producer: `/tmp/js-wf-batched-invariant-native-20261004`.
Original stores were not independently reopened; some intentionally end in a
corrupted state for negative controls. This is focused full-audit qualification,
not a full fault matrix, large-population guarantee or24-hour pass. The modeled
snapshot checker bytes remain unchanged at the accepted Tier1 source reference.
