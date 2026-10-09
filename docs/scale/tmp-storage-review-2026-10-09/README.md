# Temporary storage review — 2026-10-09

At initial inspection `/tmp` held about 151 MiB, rising to 165 MiB as the active worker race fixture grew. The active Go build directory (77 MiB), worker plugin (26 MiB), and native NATS fixture (35–49 MiB) must remain until that job closes. Active Claude/Codex sockets and state, the original plan attachment, and historical model sources referenced by reviewers are retained.

The inactive `/tmp/js-wf-cas-baseline-topology` worktree occupied 7,217,152 allocated bytes and contained a modified benchmark plus an untracked `topology.go`. Its entire checkout was uploaded to S3. A full download matched the compressed SHA-256; every archived file matched the unchanged original inventory. `receipt.json` records the object key, original Git revision, status, and file hashes. `uncommitted.patch` also preserves the tracked edit in Git. A privileged process/descriptor/memory-map/mount scan found no references or permission gaps before retirement.

## Restore

Download the bucket/key in `receipt.json` using the supplied S3 endpoint. Check its SHA-256 against the receipt before extraction. Extract into a fresh directory; the archived `.git` file is an old local worktree pointer. For a working Git checkout, create a fresh worktree at the receipt's `git_head`, then restore the archived files except `.git`. This preserves both tracked edits and the untracked benchmark source.

The scripts in this directory capture the executed upload and inactivity check. The removal record is appended after local retirement.

## Retirement completed

After recovery receipt commit `e8cb99e` was pushed, a fresh S3 download matched the archive SHA-256, the source files still matched the inventory, and a fresh privileged inactivity scan passed. Git removed the dirty worktree using `git worktree remove --force` only after these checks. The transfer archive was also removed. `removal.json` records 7,217,152 source allocated bytes reclaimed (about 6.9 MiB). `/tmp` then measured 156 MiB; almost all remaining space was still used by the active race fixture and its build/plugin files. The root filesystem had about 56 GiB available.
