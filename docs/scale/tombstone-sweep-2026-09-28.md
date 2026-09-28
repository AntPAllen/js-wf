# Bounded tombstone cleanup

`retention.TombstoneScan` pages over the underlying `KV_WF_STATE` stream by
sequence. A missing sequence counts toward the budget, so purged KV history
cannot hold the cursor. For each current tombstone revision, it checks expiry
and the retained invocation generation before deleting with a KV revision
condition. Dry-run pages report eligible keys without deleting them.

`reconcile.RunTombstoneLoop` elects one leader with `WF_LEASE` and persists its
cursor in `WF_STATE`. The cursor itself adds a KV stream sequence on every
page, so the loop requires a budget of at least two. The operator CLI exposes
`scan-tombstones` for a bounded dry run (`-apply` deletes) and
`tombstone-loop` for scheduled cleanup. The older `sweep-tombstones` command
still performs a one-shot full key scan.

An opt-in three-node test stored 100,000 expired tombstones, ran the leased
loop with 256-sequence pages and a 10 ms interval, and verified from another
node that no live tombstone keys remained. The full test passed in 61.4
seconds, including fixture setup; 100,000 keys were stored in 1.3 seconds
and cleanup was complete by 58.4 seconds from the start. A 1,000-key
diagnostic and race-instrumented run passed. Separate regular tests cover
dry-run behavior, page boundaries, sequence holes, an old invocation still
retained, and a newer KV revision replacing a tombstone.

```sh
WF_TOMBSTONE_SWEEP_SCALE=1 go test ./integration \
  -run '^TestHundredThousandTombstonesSweptByLeaderLoop$' \
  -count=1 -timeout=15m -v
```

Set `WF_TOMBSTONE_SWEEP_COUNT=1000` for a smaller run. The test measures
cleanup of expired tombstones with no live client traffic; concurrent reuse
and purge ordering are tested separately. Blob reclamation still requires a
quiescent runtime.
