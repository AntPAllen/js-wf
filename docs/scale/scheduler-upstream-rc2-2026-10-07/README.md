# Official RC2 still reproduces scheduler-cleanup persistence defect

At recorded clean `da8dc7dd31bb9930b6d2cb63bfe9a5c95b0f8aeb`, official [`v2.15.1-RC.2`](https://github.com/nats-io/nats-server/releases/tag/v2.15.1-RC.2) reproduces the same fresh two-message missing-source cleanup defect as stable2.15.0 and RC1. The expired schedule disappears in memory but returns after clean file-store close/reopen. The exact copied dirty-count control persists its removal. No production dependency changed.

| Compiled RC2 source | Before loop | After loop | After reopen | Callbacks | Actual named/package verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Unchanged official module | 1 | 0 | 1 | 0 | Expected failure, 0.009s package |
| Exact copied dirty-count control | 1 | 0 | 0 | 0 | Pass, 0.019s package |

Both preserve the ordinary anchor and physical message count1/last sequence2. The callback is a recorder: no server, Raft, workflow or target-publication path runs. Compilation wall time is separate. Original million stores are not used, indexes are not deleted, and no original store is reopened. This confirms the narrow cleanup behavior, not the original million campaign's physical-retirement cause or its drain/release gate.

Go records module checksum `h1:5dyJEGdG+OswDdiEvw06W7BukgvHbJEW8OrikvMbbIs=` and tag commit `d564fd6982a44cc47c4228b12f7a9b6c9f722a8c`. [Official tag provenance](official-tag.json) retains the annotated tag chain and resolves it to the exact same commit. [Independent review](independent-review.json) verifies all598 upstream files, both complete compiled inventories, exact runner/fixture bytes against execution Git source, raw named/package verdicts, retained stores and unchanged production `go.mod`/`go.sum`.

[Compatibility/provenance controls](provenance-controls.json) accept all three complete recorded-source corpora (stable/RC1/RC2), reject15 substitutions, and prove all reviewed original bytes unchanged. Stable and RC1 were restored from their complete S3 proofs before read-only review; no native rerun or broker/store process was started. The initial compatibility attempt pointed at already-trimmed legacy roots and failed before controls; that error is retained. Initial tag capture assumed a direct commit ref; corrected capture reads the documented annotated-tag chain. Neither correction reruns the native.

Manual scheduler CI now offers RC2 explicitly; its default remains stable2.15.0. No hosted cleanup run is dispatched. Release notes about replication and recovery do not establish scheduling correctness. The original million physical-drain gate, full matrices, onlineGC and full-runtime24h remain open. The isolated original24h producer/observer/reviewer remain active.

Complete original evidence: 1240 members/8,690,915 bytes/SHA256 `648d1a6a4b6b3bbbe67a8997093b09a48ca5cdbf8ff553a1aa20de624444d86f`. Every member and all unchanged current files are read back. [Capture and closure](capture.json) binds the retained terminal unit/exit0 and visible process/thread-FD/Docker/mount/loop checks, including permission limits. All copied upstream sources and both original fresh stores are in the full archive, with original file modes/mtimes. Compiler dependency provenance is not exhaustive/hermetic; no broker or full-scale qualification is inferred.

## Reproduction and restoration

Run `scripts/check-nats-scheduler-cleanup.py --module-version v2.15.1-RC.2 --root /tmp/fresh-cleanup-rc2`, then `scripts/review-nats-scheduler-cleanup.py /tmp/fresh-cleanup-rc2 --expected-version v2.15.1-RC.2 --require-retained-store`. Fresh roots only.

Use `s3-readback.json` to download the full archive, then `scripts/restore-full-fixture-proof.py` with this directory's archive metadata/inventory into a fresh destination. No process starts automatically and no verdict changes. Production remains pinned to2.15.0.
