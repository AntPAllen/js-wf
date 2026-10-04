# Audit consumer leader loss: independently accepted focused case

Executed `7d5d0b0d73eb624862e81faa86da1dec89a3a33f`, actual retained race SDK.
The test reads the temporary consumer's actual leader metadata after128 visits
and shuts down that real in-process NATS node. This is distinct role targeting;
it is not an OS SIGKILL or worker-kill test.

All3000 retained records match the point baseline digest. Observed consumer leader0,
consumer replicas3, elapsed2.031391228s,2488 leader reads recover the unvisited tail,
zero reconnects on this actual client and zero consumers after cleanup. Audit uses
its original20-second context. The whole test passes8.14s (package9.160s).
This does not prove that a very large serial fallback tail will fit the budget.

All2889 captured inputs /59 Git-local files match pre/post/executed Git bytes.
Actual SDK SHA and every build-info field verify:
`7d3342e083e89021f909a97a0ee627757a14ebf803a68988a08a0367fb2197bb`.
Actual runner bytes also match executed Git. Complete2962-member /
88387326-uncompressed-byte proof retains SDK, selected local/module/toolchain
sources, raw result logs and original native stores. Archive32125725 bytes, two
ordered parts. All member hashes/unchanged originals/part hashes/concatenated SHA
verify; see `manifest.json`.

Concatenate ordered parts, verify hashes, extract into a fresh directory, then:

```bash
python3 review.py --root /path/to/extracted/producer --repo /path/to/js-wf --output /tmp/consumer-leader-review.json
```

Original producer: `/tmp/js-wf-batched-consumer-leader-20261004`.
Stores not independently reopened. This is focused reader qualification; it does
not qualify a full real-cluster matrix,24-hour row or large-population capacity.
Default audits stay on point reads; explicit soak mode chooses batching for both
checkpoint and final invariant checks while retaining the original20s/60s limits.
