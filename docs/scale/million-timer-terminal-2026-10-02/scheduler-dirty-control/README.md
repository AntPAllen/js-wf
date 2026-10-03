# Copied-store scheduler cleanup persists with a dirty-count control

The pinned NATS 2.15.0 [original experiment](../scheduler-resume-boundary/)
cleared missing-source schedules in memory, but they returned after a clean
close and reopen. This experiment changes only a fresh copy of
`server/filestore.go`: `runMsgScheduling` increments `fs.dirty` when
`getScheduledMessages` changes the scheduler map cardinality.

The same committed fixture executes all six copied-store cases successfully.

| Copy | Before scheduler loop | After loop | After second reopen | Target callbacks |
| --- | ---: | ---: | ---: | ---: |
| Rebuilt node 0 | 768 | 0 | 0 | 0 |
| Rebuilt node 1 | 141 | 0 | 0 | 0 |
| Rebuilt node 2 | 0 | 0 | 0 | 0 |
| Intact nodes 0/1/2 | 0/0/0 | 0/0/0 | 0/0/0 | 0/0/0 |

All 1,818 exact source purges and absence checks pass. All six final physical
message counts are zero, with first sequence 2,000,001 and last sequence
2,000,000. The actual Go package passes in 0.09 seconds after compilation.

The independent reviewer verifies all 4,998 original campaign file hashes,
all 598 upstream module files, the exact single-file compiled change, fixture
and runner hashes, six actual subtest passes and the package pass. It also
reads back every member of the original rejected experiment's archive and
checks its six baseline measurements against the unchanged fixture.

This is causal evidence for the narrow cleanup persistence behavior in these
isolated copies. The experiment directly invokes the file-store scheduling
loop with a recording callback. It runs no server, Raft or publish path, and
does not confirm why the original million-timer campaign missed source
retirement. The production NATS dependency and original stores remain
unchanged. The million-timer 24-hour drain gate remains open.

`originals.tar.gz` retains 57 evidence members, including the exact reviewer
and runner, full baseline/modified source, raw Go events, all six snapshots,
source inventories and the initial rejected setup. That setup failed while
writing a read-only copied source file, before any Go test execution; the fresh
qualified attempt makes only the copied file writable. Every archive member
was SHA256 checked by readback before atomic publication. Physical copies and
copied build trees remain at the paths in `manifest.json`.

To review retained local experiment bytes:

```sh
tar -xOf originals.tar.gz review.py > /tmp/review-scheduler-dirty-control.py
python3 /tmp/review-scheduler-dirty-control.py \
  /tmp/js-wf-million-scheduler-dirty-qualified-20261003
```
