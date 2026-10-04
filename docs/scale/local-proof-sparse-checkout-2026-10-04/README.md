# Reversible local checkout reduction for committed proof archives

At exact `c5348878e0664fe053048ee4e04d0f4a101d5158`, root disk had about
300 MiB free. The main local worktree now omits **55 already-committed proof
archives of at least 16 MiB** under `docs/scale/`. This removes duplicate
working files while preserving the complete proof bytes in local Git objects
and the pushed repository. Every selected archive's materialized size, SHA256,
Git blob identity and independently streamed Git blob SHA256 matched before
omission. None was open in a process file descriptor.

The worktree-specific non-cone sparse patterns include everything except
these exact recorded paths. All **767** tracked `.go`, `.py`, `.yml`, module
and checksum inputs remain materialized and hash-identical before/after;
Git HEAD and clean status are unchanged. Other worktrees retain their checkout
configuration. The complete script suite passes **156 tests in 3.688 seconds**,
including tests that use the retained small documentation fixtures.

Recovered allocated bytes: **1,950,371,840** (about 1.82 GiB). Root free space
rises to about **2.2 GiB**. This provides room for forthcoming shard proofs;
it is not a capacity qualification for the original full 24-hour soak.
Physical broker stores and failed originals under `/tmp` or RAM are untouched.
Existing runtime, history, matrix and release qualification remains unchanged.

`proof.tar.gz` retains exact executed verifier, all source pre/post hashes,
archive/blob verification ledger, applied sparse patterns and full script-test
log. Every outer member was independently read back and hash-verified. The
sparse setting is local only; no GitHub workflow is changed by this operation.
The executed verifier records this one recovery and its exact baseline, rather
than providing a reusable general cleanup command.

## Restore archived working files

The recorded paths are absent only from this local worktree. Git can still
read their exact bytes at any retained revision, for example:

```sh
git show HEAD:docs/scale/current-tier2-matrix-2026-10-03/journal-1-12/originals.tar.gz > /tmp/journal-1-12-originals.tar.gz
```

Check the saved SHA256 ledger before using restored evidence. To restore the
whole working checkout, ensure roughly 1.9 GiB is available and run:

```sh
git sparse-checkout disable
```

This rematerializes the original committed files. It does not require fetching
or rewriting broker data. Future reviews requiring a recorded archive path
must restore that input first; an absent working file is not lost evidence.

## Subsequent archive additions

[The additional recovery](extension/) omits 28 newly committed archive working files after exact blob verification, recovering another 683,163,648 allocated bytes. All 776 current tracked source inputs remain unchanged. The complete checkout now requires room to restore all 83 omitted archives (about 2.45 GiB total), rather than only the original 55. All archive bytes remain retained in local/pushed Git.
