# Official upstream RC scheduler-cleanup diagnostic

The official `v2.15.1-RC.1` module reproduces the same fresh two-message missing-source cleanup persistence defect as stable2.15.0. Clean execution source is `de2f805b1e008a88fa6f9264605c1dceb5100ca6`; Go records upstream tag commit `45e4740ca2a95953c6a5a284551f88aa22c26309`, module checksum `h1:0E7+w0EkLo/nMFDCHIveI0wZ7sEtYC571HnGWl71G3Y=`, and unchanged production `go.mod`/`go.sum`.

| Compiled RC source | Before cleanup | After cleanup | After reopen | Callbacks | Actual package verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Unchanged official module | 1 | 0 | 1 | 0 | Intended named failure, 0.019 s |
| Exact copied dirty-count control | 1 | 0 | 0 | 0 | Pass, 0.010 s |

Both preserve the ordinary anchor and physical last sequence2. The control modifies only copied `server/filestore.go`: mark the scheduler state dirty when expired-message processing changes schedule-map cardinality. There are no original campaign stores, index deletion, server processes, Raft or workflow operations in these tests. The full module cache remains unchanged; independent review verifies all598 upstream source files, both complete compiled inventories, exact runner/fixture bytes against execution Git source, raw named/package verdicts and both retained stores.

Reviewer source `8a85562` also accepts the earlier clean stable corpus and rejects fourteen isolated provenance substitutions. Those checks read existing evidence without modifying it or reopening stores. Missing or inconsistent dependency ledgers, production-pin changes, wrong upstream module/version/origin/tag, download errors and malformed checksums are rejected. The earlier uncommitted initial stable runner remains ineligible for Git-bound review; the positive compatibility corpus is the clean complete-originals run at `bac9356`.

Reproduce with a fresh root:

```sh
python3 scripts/check-nats-scheduler-cleanup.py --module-version v2.15.1-RC.1 --root /tmp/cleanup-rc-fresh
python3 scripts/review-nats-scheduler-cleanup.py /tmp/cleanup-rc-fresh --expected-version v2.15.1-RC.1 --require-retained-store
```

The manual CI workflow exposes an explicit diagnostic version; its default is stable2.15.0. No additional hosted cleanup run is launched for this local result. Production dependencies are unchanged. This establishes that the selected official RC does not fix the narrow cleanup persistence defect. The original million-timer missed physical retirement cause and original full-volume drain gate remain unconfirmed and unqualified; this control is not a production repair recommendation.

The complete original root is archived with per-file hashes, modes and nanosecond mtimes; all archive members and unchanged current files are checked. `capture.json` records terminal unit exit0 and fresh visible process/thread-FD/container/mount/loop closure, including inspection permission limits. Restore into a fresh directory with `scripts/restore-full-fixture-proof.py`; then run its retained reviewer with the explicit RC version and repository path. S3 transfer/readback is recorded separately in `s3-readback.json` after publication.
