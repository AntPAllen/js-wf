# Durable-receipt million campaign: first restart checkpoint

The authoritative `js-wf-timer-million-20261001.service` remains active with
MainPID18146, source92586ea3ff579362b5ab61e7026b6d1f244a2d90 and
`source_modified=false`. Its first actual all-three-server SIGKILL begins
2026-10-02T00:40:35.705480970Z after333344deliveries and heals at
00:40:50.847776533Z:15.142295563s. The report records distinct killed PIDs18193,
18194 and18195, all with SIGKILL. Receipts continue after heal.

`running-report.json`, `progress.log` and `service-state.txt` are read-only
mid-run snapshots with SHA256 hashes, not a terminal acceptance report. The
campaign still needs its second full restart near two-thirds, all1M receipts,
final p99/max lateness gates, durable-ledger verification and physical drain.
The live source paths and service remain authoritative; do not restart or
replace this process merely because the checkpoint is incomplete.
