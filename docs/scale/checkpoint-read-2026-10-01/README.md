# Prefix-free checkpoint resume reader

The modified tree based on `586cc5d` adds `journal.ReadCheckpoint`. It validates
manifest shape, frame hash/generation/stage/position, the retained completion
anchor and contiguous suffix indices/epochs. It returns frame bytes and only
records after that anchor. It never downloads the archived prefix. Nil means
absent metadata or an ordinary version-1 snapshot; corrupt metadata, malformed
frames and generation mismatch cannot become initial replay. Unknown metadata
fails immediately. Missing/gapped live scans retry the manifest within the
bounded whole-read recovery window to handle a newer checkpoint purging an
older anchor. A final manifest check prevents returning a superseded pointer.

Long suffixes use the existing batch cursor after 64 serial reads, with the
checkpoint's logical index as the base. Ordinary full reads retain base zero.
The audit/offline reader still reconstructs the complete archived journal.

Proofs:

- The real three-node R3 file-store contract passed under race in 6.214 seconds.
  Its port denies every archive-object read. A restored SDK context observes
  materialized state and executes 150 effects, then another prefix-free read
  replays all 150 outcomes without executing their functions. Returned records
  exactly match the retained control suffix. The contract also checks two
  checkpoint publications, anchor preservation, full logical reconstruction,
  generation rejection and quiescent blob reachability/corruption handling.
- Ten seeded modes include missing anchor, deleted suffix entry, epoch regression,
  terminal-plus-extra-entry, frame corruption, generation mismatch, named
  transport retries, checkpoint advance/purge during a read, and a 300-entry
  batch suffix. Exact and cross-process trace replay passed under race (sim
  18.392 seconds; metadata/cancellation unit controls 1.023 seconds).
- 100,000 generated schedules, 100,000 choices and 28,229,366 transport events
  passed in 106.879 seconds. Maximum modeled delay was 2,000 ms; raw coverage
  summary is retained.
- The final complete simulator suite, including the new seed-42 pin, passed in
  89.245 seconds. Vet and diff checks passed.
- An overlay of production `checkpoint_read.go` that passes base zero to its
  batch reader makes pinned long-suffix seed 42 fail: expected index 65, got 69.
  This was a compiled failing test, not a build failure. The normal pin passes
  in the full simulator suite. The overlay was not applied to the worktree.

Commands: `go test -race ./integration -run '^TestCheckpointManifestRetainsAnchorAndPromiseBlobs$' -count=1 -v`,
`go test -race ./journal ./sim -run 'TestCheckpointRead|TestSeededCheckpointReadReplay' -count=1 -v`,
`SIM_SEEDS=100000 SIM_COVERAGE_SUMMARY=1 go test ./sim -run '^TestSeededCheckpointReadReplay$' -count=1 -v`,
and `go test ./sim -count=1 -timeout=5m`.

This is a direct SDK/storage contract, not production worker dispatch. Workers
still use full replay. Stage registration, Continue's frame/pair creation,
lease-fenced dispatch and append counters, handoff/reconciliation, offline
continuation replay, retention/reuse cuts and the final matrix remain open.
The real fixture's 150 effects do not prove a worker process skipped its initial
handler; that gate still needs the worker integration.
