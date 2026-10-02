# Retained timer sources remain addressable and purgeable

The copied-store runner's `--probe-subject-purge` option exercises unchanged
NATS 2.15.0 file-store subject lookup and purge after recovery. All six actual
cases pass in 0.902 seconds of test execution.

| Replica | Intact index: sources found and purged | Rebuilt index: sources found and purged |
| --- | ---: | ---: |
| node 0 | 768 | 768 |
| node 1 | 141 | 141 |
| node 2 | 0 | 0 |

All 1,818 probes find the exact expected sequence through `LoadLastMsg`, remove
exactly one source through `PurgeEx(subject, 0, 0)`, and verify direct/subject
absence afterward. Every copied store finishes with zero messages and last
sequence 2,000,000. Direct lookups report 1,642 “no message found” and 176
“deleted message” outcomes; subject lookups uniformly report “no message found”.
Scheduling remains paused, with no server, Raft group, callback, workflow
handler or target publication. All 4,998 original campaign files remain byte
identical according to before/after SHA256 inventories.

This rules out a persistent inability to locate or purge these retained records
after this recovery. It does not rule out transient failures during the original
campaign, explain why source retirement was missed, or repair any original
store. The million-timer release verdict remains failed.

Two attempts are rejected and preserved: the initial probe incorrectly required
“no message found” for a directly deleted sequence, and its first correction
used a nonexistent exported error name. The final fixture recognizes the pinned
module's private `errDeletedMsg`, missing-record and EOF errors while requiring
subject absence and final zero physical messages. Build failure is rejected by
the runner, and contributes no positive evidence.

All 65 qualified/rejected top-level evidence files are archived losslessly in
`originals.tar.xz`, with SHA256 readback before atomic publication. XZ reduces
repeated full stream-state deleted-sequence lists without dropping raw fields.
Physical copies and upstream build sources are preserved locally and excluded
from the published archive. See `manifest.json` and `independent-review.json`.
Original roots are `/tmp/js-wf-million-subject-purge-actual-20261002`,
`/tmp/js-wf-million-subject-purge-20261002`, and
`/tmp/js-wf-million-subject-purge-qualified-20261002`.
