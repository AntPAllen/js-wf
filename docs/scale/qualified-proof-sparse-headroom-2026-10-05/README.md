# Qualified proof sparse headroom — 2026-10-05

The existing main-worktree sparse checkout was extended by169 exact exclusions
for accepted Tier2 all-server/consumer and Tier3 disk-delay archive parts.
Before the change, every present part matched its manifest, pushed83682bb tree
blob identity and independently streamed local Git object SHA256. Every manifest
records prior complete member readback. The50 selected directories have exact
numeric seed ranges; aggregate qualification directories are excluded.

Recovered3650080768 allocated bytes from worktree copies; filesystem available
space increased from3578347520 to7228076032 bytes during the operation. All Git
objects remain readable and Git status was clean immediately afterward.
No visible selected-file descriptors were open. Existing sparse patterns were
preserved. Native fixtures, original ZIPs and `/tmp` canonical archives are
unaffected, including failed and live originals. This changes storage headroom,
not test verdicts or source qualification.

The first selector also matched the aggregate `cluster-full200-qualification`
directory and rejected its different manifest shape before any mutation. The
corrected numeric-range selector completed; both scripts are retained.

## Restore a part for inspection or replay

Use its exact `path` from `executed-sparse-recovery.json`:

```sh
git restore --ignore-skip-worktree-bits --source=HEAD -- 'docs/scale/current-tier3-block-delay-2026-10-04/seeds-157-169/proof.tar.gz.part-*'
```

Verify restored part hashes against the directory's manifest before assembling
an archive. `git sparse-checkout reapply` removes these worktree copies again.
The local canonical Git blobs and pushed repository contain every byte.
`prior-patterns.txt` and `new-exclusions.txt` record the exact pattern change.

The two24h campaigns shared the VM during the readback; resource overlap does
not establish any native failure cause. Their original live directories were
not archived or restarted by this operation.
