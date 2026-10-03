# Fresh two-message scheduler cleanup reproduction

The missing-source cleanup behavior previously observed in copied million-timer
stores now reproduces with a newly created file store and only two messages.
No campaign data, scheduler-index deletion or injected map entries are required.

The fixture creates an already-expired scheduled source and an ordinary anchor,
closes/reopens the store, purges the source by exact subject, and closes/reopens
again. It then invokes the production expired-scheduling loop with a recording
callback and performs another clean close/reopen.

| Compiled source | Before loop | After loop | After reopen | Callbacks | Physical messages |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unchanged NATS 2.15.0 | 1 | 0 | 1 | 0 | 1 anchor |
| Copied source with dirty-count change | 1 | 0 | 0 | 0 | 1 anchor |

The baseline must fail the actual named assertion that removed scheduler entries
remain absent after reopen. The identical fixture must pass with the exact
single-file dirty-count change, preserve anchor payload and last sequence2,
and produce no target callback. The runner rejects skipped tests, build failures,
unrelated failures and incomplete runs.

Run the diagnostic from the repository root with a fresh output directory:

```sh
python3 scripts/check-nats-scheduler-cleanup.py \
  --root /tmp/js-wf-scheduler-cleanup-fresh
```

It copies the pinned module into two separate build directories and verifies
every upstream file. Only the control's copied `server/filestore.go` changes:
`runMsgScheduling` increments `fs.dirty` when processing changes scheduler map
cardinality. The module cache and production dependency remain unchanged. Both
cases retain commands, actual working directories, environment, full compiled
inventories, raw Go JSON and exact fixture/runner bytes.

This establishes a narrow file-store cleanup persistence defect and its causal
control. The fixture uses direct file-store operations and explicitly paused
scheduling; it runs no server, Raft, public scheduling API or workflow. It does
not explain why the original million-timer campaign missed source retirement,
repair its stores, or clear its 24-hour drain gate. Compilation is separate from
the millisecond test execution.

The reusable runner itself was executed and independently reviewed: baseline
package0.008s fails the intended named assertion; control package0.010s passes.
All598 upstream module files and both complete compiled-source inventories are
checked; the module cache remains unchanged. Executed fixture/runner hashes
match the repository files. Execution metadata honestly records an uncommitted
new diagnostic at base62f3f25; it is not a clean whole-repository qualification.

`originals.tar.gz` preserves the initial manual pair and the qualified runner
pair, raw Go events, complete source inventories, exact patch/fixture/runner
bytes, commands and reviewer. Every member was SHA256 checked by readback
before atomic publication. Full copied module trees remain at the paths in
`manifest.json`; each synthetic store was created in Go's `t.TempDir` and
cleaned after its test completed.
