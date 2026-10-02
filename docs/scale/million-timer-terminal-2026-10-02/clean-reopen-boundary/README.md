# Clean reopen preserves physical purges but retains rebuilt schedule entries

The new `--probe-reopen` option runs the subject-purge probe, checks a clean
file-store stop, opens a fresh file-store handle, and checks every source again.
The actual pinned NATS 2.15.0 test **fails** its zero-schedule assertion on the
rebuilt-index node 0 and node 1 copies. This trial is rejected, not a passing
drain qualification.

| Copy | Physical messages after reopen | Scheduler entries after reopen |
| --- | ---: | ---: |
| Intact index, nodes 0/1/2 | 0/0/0 | 0/0/0 |
| Rebuilt index, nodes 0/1/2 | 0/0/0 | 768/141/0 |

All 1,818 known source checks remain absent through both sequence and subject
lookup after reopening. Every copy retains last sequence 2,000,000. Scheduling
stays paused throughout; there is no server, Raft group, callback or publication.
This shows durable physical removal in this explicit purge experiment, while
the rebuilt schedule index retains entries. It does not establish the cause of
the original campaign's missed retirement or prove full-server behavior.

All 4,998 original campaign file hashes match before and after. The runner and
fixture bytes match the executed copies. The 31 top-level evidence files are
archived losslessly with SHA256 readback before atomic publication; physical
copies and the upstream build directory remain at the original root recorded
in `manifest.json`. Reopen-mode state artifacts explicitly use `counts-v1`
(messages, first sequence and last sequence); earlier full stream-state
snapshots remain in the separate subject-purge archive.

The failed assertion remains in the fixture. No original store was repaired,
and no retry was run to obtain a passing verdict.
