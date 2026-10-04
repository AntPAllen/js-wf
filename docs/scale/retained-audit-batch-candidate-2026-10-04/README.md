# Retained audit bulk scan candidate

The production audit still uses its existing exact-sequence reader. A test-only
candidate now uses an ephemeral, memory-backed, unfiltered pull consumer to read
batches, retaining the captured first/last bounds and invocation cutoff. Consumer
sequence gaps and omitted tails are resolved with exact leader GetMsg reads;
consumer delivery alone never proves absence. Records found through gap reads
are visited in order. Semantic failures, duplicates, wrong streams and wrong
sequence responses fail the scan. Fetch contexts remain bounded by2 seconds and
the unchanged outer audit context. Consumers are explicitly deleted; inactivity
expiry is a secondary cleanup mechanism.

## Observed results

A race test on three real in-process NATS nodes published35,000 file-backed,
three-replica records, deleted seven entries (including first/interior/captured
last sequence), and published20 messages beyond the cutoff. Both scans visited
34,993 retained records with identical ordered digests of sequence, timestamp,
subject, proof header and payload:

- Existing32-reader scan: **12.758667811s**.
- Candidate512-message batches: **1.869967735s**.
- Zero consumers after explicit cleanup.
- A second fresh candidate scan observed another deletion made after the first.

The full native test passed in44.93s (race package45.960s). The initial attempt
never reached scanning: its unbounded setup request timed out before cluster
readiness. Setup now uses bounded readiness calls. That failed setup is not a
reader performance result.

Final-source race controls pass1.015s: missing consumer records and tail must be
read back; actual holes are tolerated; duplicate delivery, wrong stream, semantic
GetMsg/batch/visitor errors and wrong gap sequences fail; cancellation is
preserved and cleanup is exercised. The real-cluster test is opt-in so ordinary
unit/simulator runs remain fast:

```bash
WF_AUDIT_BATCH_CANDIDATE=1 go test -race ./integrity -run '^TestAuditBatchScanCandidateNativeHighWaterHolesAndReadback$' -count=1 -v
go test -race ./integrity -run '^TestAuditBatchScanCandidate(Resolves|Rejects)' -count=1 -v
python3 docs/scale/retained-audit-batch-candidate-2026-10-04/review.py --repo "$PWD" --output /tmp/batch-reader-review.json
```

## Scope and next qualification

This is a candidate reader experiment, not a full invariant audit, fault-matrix
pass or24-hour qualification. Native source and logs are retained; the successful
native executable and original stores are not retained. `review.py` proves the
current reader helper bytes match the executed helper; the native opt-in guard
was added after that comparison. It verifies recorded logs, not independent
replay of those deleted stores. Timing is one comparison, not a general speedup
or an extrapolated24-hour capacity guarantee.

Before replacing production reads: qualify leader loss, delayed/missing consumer
responses, interruption and cleanup, old supported server compatibility, sparse
large spans and large retained populations. Handle uncertain consumer creation
with stable identity to avoid duplicate consumers. Then wire equivalent audit
instrumentation and exercise full invariant checking, including terminal-state
validation, under the original20s/60s limits. No Tier1 runtime producer bytes or
production audit settings changed for this experiment.
