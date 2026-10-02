# Expired scheduler cleanup is observed but returns after a second reopen

The `--probe-scheduler-resume` option extends the copied-store purge/reopen
probe. After checking physical absence, it registers a recording callback,
verifies all retained schedules are expired, unpauses scheduling under the
file-store lock, and directly invokes the production `runMsgScheduling` loop.
It then checks a clean stop and opens another fresh file-store handle.

The actual pinned NATS 2.15.0 trial is **rejected**: rebuilt-index nodes 0 and 1
fail the durable cleanup assertion. All six cases actually execute.

| Copy | Entries before loop | Entries after loop | Entries after second reopen | Target callbacks |
| --- | ---: | ---: | ---: | ---: |
| Rebuilt node 0 | 768 | 0 | 768 | 0 |
| Rebuilt node 1 | 141 | 0 | 141 | 0 |
| Rebuilt node 2 | 0 | 0 | 0 | 0 |
| Intact nodes 0/1/2 | 0/0/0 | 0/0/0 | 0/0/0 | 0/0/0 |

All final physical message counts remain zero, at last sequence 2,000,000.
All 1,818 exact source purges and absence checks pass before resume. The expired
loop clears stale entries in memory without generating any target callback,
but the clean close/reopen does not preserve that cleanup in this experiment.

Source inspection provides a possible explanation for this narrow result:
`getScheduledMessages` removes missing-source entries; `runMsgScheduling` does
not increment the file-store dirty counter for those removals. `Stop` requests
a full-state write, documented as a no-op when not dirty; scheduling state is
written through the full-state path. This is an inference from pinned local
source, not proof of the original million-timer persistence cause.

This directly invokes the scheduling loop with a recording callback. It does
not run a server, Raft, the normal server recovery lifecycle or a publish path.
The default `--probe-reopen` still enforces zero entries before resume; the new
option adds explicit before/after/second-reopen measurements and its own strict
durability assertion. Neither failed result was changed into a passing verdict.

All 4,998 original files remain byte identical. Executed runner and fixture
hashes match the committed versions. All 37 top-level evidence files are
archived losslessly with SHA256 readback before atomic publication. Counts-v1
snapshots declare their reduced fields; earlier full stream states remain in
the subject-purge archive. Physical copies and build sources remain at the
original root in `manifest.json`. No original store was repaired.
