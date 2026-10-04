# Replicated bulk audit reader candidate: retained native qualification

Executed source `097c2756444c56002cfe67ce19eab5c636da2d40`, actual retained race
SDK. Candidate remains test-only; production audit still uses its existing
reader. Original20-second attempts and60-second total cap are unchanged.

## Native results

| Case | Observed result |
| --- | --- |
| Stream leader loss on three real in-process nodes | All3000 records match baseline digest;258.261756ms, consumer replicas3, zero gap reads and zero consumers after cleanup. Audit client stays on a surviving node. This is real NATS node shutdown, not OS SIGKILL. |
| Explicit cancellation | Exactly128 visits, `context canceled`,99.488017ms including cleanup, zero consumers. No later visits. |
| Consumer deletion during delivery | All3000 records match baseline;2.854176880s,2488 leader reads recover the unvisited tail, zero consumers. The test allows explicit failure as a safety outcome, but this actual run completes successfully. |
| Three NATS2.11.17 processes |997 retained records after three deletions match baseline;1.888964078s, three leader gap reads, actual returned consumer replicas3, zero consumers. Executed legacy server is retained. |
|100000 deleted interior sequences | Captured span100002, two retained anchors match baseline;1.927674630s and exactly one leader next-message query verifies the entire deleted gap; zero consumers. |

All omission/order/semantic/uncertain-create/transport controls also pass in the
same actual executable. Consumer creation reuses one name across uncertain
replies; it explicitly requests stream-matched replication. Named transport
failures recover through leader next-message reads at the next unvisited position;
semantic errors still fail. Cleanup can proceed after explicit cancellation but
never extends the original deadline. The whole actual run takes50.027372449s.

## Independently preserved proof

All2884 selected captured inputs /54 Git-local files, original pre/post hashes,
actual runner bytes against Git, actual SDK SHA/all build-info fields verify.
Actual SDK SHA:
`5a71b4fc90904d8579c43039aea3d396225dc06f7e163b68886cdc6eba67d918`.

Complete3181-member /165054918-uncompressed-byte proof retains actual SDK,
selected local/module/toolchain bytes, raw reports, original native stores/logs,
executed legacy NATS binary and the modern server built by the legacy fixture.
That built modern server was not executed; modern fault cases run inside the SDK.
All member hashes, unchanged inputs, three part hashes and concatenated SHA
verify. Archive68093646 bytes; see `manifest.json`. The input inventory is a
selected superset, not exhaustive assembly/embed/generated/hermetic provenance.

Concatenate parts in numeric order, verify hashes, then extract into a fresh
directory. With Go and the executed revision available, run:

```bash
python3 review.py --root /path/to/extracted/producer --repo /path/to/js-wf --output /tmp/bulk-reader-review.json
```

The reviewer binds captured bytes and named outcomes to the actual executable;
it does not independently reopen original stores or reproduce faults. Original
producer: `/tmp/js-wf-batch-candidate-replicated-20261004`.

## Remaining work

This qualifies the focused raw reader cases only. Full invariant audit wiring,
terminal-state/snapshot reads, large retained-population capacity, prolonged
partitions/no-quorum, OS SIGKILL, and actual24-hour qualification remain. Earlier
35k performance numbers apply to834daa8, not an unmeasured general speedup of this
version. The older failed leader-loss run stays separately preserved; its exact
server-side cause is not inferred solely from the corrected run passing.
