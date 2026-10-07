# Verified scale-fixture offload — 2026-10-07

Reclaimed **7.80 GiB** (8,378,458,112 allocated bytes). After cleanup `/tmp` uses **7.53 GiB**; filesystem free space is **83.75 GiB**. These are observations while the retained 24-hour test continues to grow.

| Fixture | Reclaimed, including staging archive where present | Recovery |
| --- | ---: | --- |
| `js-wf-bulk-latency-full400k-valid-20261006` | 3.748 GiB | [S3 receipt](../../full400k-bulk-capacity-preparation-2026-10-06/native-valid-population/s3-readback.json) |
| `js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005` | 3.950 GiB | [S3 receipt](closed-capacity-donor/s3-readback.json) |
| `js-wf-online-blob-native-20261007` | 0.105 GiB | [S3 receipt](../../online-blob-boundary-2026-10-07/native-race/s3-readback.json) |

For the two 400k donors, only `fixture/` or `originals/` was removed. Their registered source worktrees, producer executables, logs and review results remain local. Each local root has `STORES_MOVED_TO_S3.json`. Future campaigns must restore stores to a fresh directory before reusing a donor. This supersedes earlier notes saying these stores remained local. The capacity donor's original failed verdict is preserved.

The completed native blob-boundary fixture and staging archive were removed in full. Its passing controlled counterexample and independent review remain in Git; it does not establish online GC safety.

Before removal, each complete remote compressed body and every member matched its committed inventory. The full current local tree also matched. Process arguments/executables/cwd, visible descriptors, Docker mounts including stopped containers, filesystem mounts and loop devices were checked again. Permission-limited observations are retained in the ledger. Registered worktrees were excluded from removed paths.

[Exact ledger](removal/removal.json), [executed removal](removal/executed-removal.py), [shared closure checks](removal/executed-common.py), [measurements and live units](removal/summary.json).

## Restore a fixture

Use the row's S3 receipt `archive_key` and `archive.url`. Download with standard S3 tools:

```sh
AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x aws s3 cp \
  --endpoint-url https://nameless-bird-8772.int.exe.xyz \
  s3://nameless-bird-8772/KEY_FROM_RECEIPT /tmp/downloaded-proof.tar.gz
python3 scripts/restore-full-fixture-proof.py \
  --archive /tmp/downloaded-proof.tar.gz \
  --metadata PATH_TO_CANONICAL/archive-verification.json \
  --inventory PATH_TO_CANONICAL/fixture-inventory.json \
  --destination /tmp/fresh-restored-fixture
```

The helper verifies the complete archive hash and every member before restoring to a fresh directory. It does not start servers. Archived source copies can contain historical `.git` worktree pointers; build from a fresh checkout of the recorded source revision. Preserve original stores and use fresh copies for experiments.

Live journal 24-hour producer, observer, reviewer and SDK PID 3743450 remained running. Million-item timer fixtures and retained block-device fixtures remain local. This cleanup changes no runtime configuration or test acceptance.
