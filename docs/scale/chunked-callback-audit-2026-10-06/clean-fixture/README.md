# Full400k cold baseline and R5 cursor-owner SIGKILL accepted

Clean02d485a normal4CPU/GOGC500/4GiB fresh verified400k/4.8M R5 file-source fixture.
Preparation validates/deletes only the two confirmed old donor cursors, records
successful replies and final named/count zero for INV/JRN, and verifies identical
retained source message/byte/first/last/deletion boundaries. No journal warmup.
Original immutable donor1058 files verified before/after.

NativePASS51.57s. Baseline exact report400k invocations/journals/terminal and4.8M
entries,4.8M once-only visits: audit18.132709081s, zero consumers18.134256542s.
Owner-down actual active R1 memory AckNone JRN cursor target killed atvisit128:
node4 source process confirmed absent, left down. Buffered old delivery drains
through24201, replacement distinct cursor starts24202. Exact once-only4.8M report,
audit16.427916994s, zero consumers16.429780366s. Old owner's delete reply hits the
shared2s cleanup deadline while replacement delete succeeds; named countzero
confirms overall cleanup. All read/recovery/cleanup remains inside original20s.
No isolated speed ratio follows from ordered cold/warmed stages.

Independent review verifies688 selected source inputs, actual SDK/five server
executables/modules/mounts/closedPIDs, preparation boundaries and unchanged donor.
Complete1698-file823alias lossless reconstruction;50,244,473byte delta SHA256
`4d149e8b936f97013ea9b18c4384e7164fa35783f448f4ba1a150819a89ab642`.
Pinned canonical donor Git parts plus delta parts required. All fixtures stay
closed. This qualifies explicit fullsize healthy/owner-left-down under recorded
configuration; same-store restart, default/live/currentmatrices/24h remain open.
