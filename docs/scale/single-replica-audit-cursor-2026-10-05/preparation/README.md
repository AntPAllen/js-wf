# Quiet full-population cursor replication comparison

Prepared diagnostic only; no native capacity result yet.

`WF_AUDIT_CAPACITY_SINGLE_REPLICA_COMPARISON=1`, together with the existing
plain comparison and verified copied-store inputs, selects callback R5 / R1 / R5.
Only temporary INV/JRN consumers change replication; persisted streams remain R5.
Each mode verifies actual consumer replica configuration and records cursor start
sequences and point reads. The candidate must return the complete 400,000 INV,
400,000 journals, 4,800,000 entries and 400,000 terminal report within its original
20-second deadline. The shared audit invariants, captured cutoffs, state reader,
gap oracle and bounded retry algorithm remain in use.

A quiet R1 result does not qualify cursor failover or authorize default adoption.
Existing replay proof requires replicated consumers; recovery, cancellation,
legacy compatibility and live fault qualification remain separate requirements.

Preparation verification: opt-in test compiles and skips without copied-store
inputs (`go test ./integrity -run '^TestConcurrentStateR5CopiedCapacityProfile$'
-count=1`). An earlier result-field placement compile error was corrected before
native execution. No capacity pass is inferred from this preparation check.

## Lossless base plus delta preservation

`scripts/fixture_delta.py` preserves all regular file bytes, paths, modes and
nanosecond mtimes from a closed fixture. Unchanged copied store files reference
matching members of a full archive at a pinned Git commit; changed and added
files are stored in full. Deleted files are absent from the logical inventory.
Ownership and directory metadata are outside the reconstruction contract.

The verifier checks every canonical base part, the complete concatenated archive,
every base member against its inventory, and the complete reconstructed virtual
file inventory. The delta requires both its pinned base Git objects and its own
parts; it is not a standalone full archive. Callers must prove process closure
before capture. Originals are never reopened by this tooling.

Verification: four seeded tests pass, covering byte/mode/mtime reconstruction,
changed/added/deleted files, corrupted working base copies, wrong base identity,
missing aliases, traversal, existing destinations and symlinks. The real committed
400k canonical fixture also reads back successfully: 1,751 data files,
3,670,153,990 bytes, complete base part/archive/member hashes verified in 10.546s.

Reconstruct with:

```sh
cat proof-delta.tar.gz.part-* > /tmp/proof-delta.tar.gz
python3 scripts/fixture_delta.py /tmp/proof-delta.tar.gz --repo /home/exedev/js-wf --restore /tmp/new-fixture
```

Retain the exact verifier source with the executed proof. Failed reconstruction
can leave a partial destination; use a new destination on each attempt.
