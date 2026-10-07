# Closed abrupt-leaf fixtures retired

Reclaimed570,036,224 allocated bytes (**543.6 MiB**) from the three completed abrupt-leaf fixtures and their staging archives. Every compressed remote body and archive member was freshly verified against committed metadata/inventory; each complete local tree still matched all recorded bytes/modes/mtimes. Expected terminal producer exits1/1/0, MainPID0/Restartno, visible process/descriptor closure, Docker mounts including stopped containers, mount and loop checks preceded deletion. Exact ledger and executed script are retained here.

Full restoration archives and inventories remain in S3 via each case's `s3-readback.json`:

- [Startup failure](../failed-startup/).
- [Inherited observer-context failure](../failed-caller-context/).
- [Accepted native SIGKILL](../native-race/).

Restore only to a fresh directory; no archived original was restarted. Failed and accepted verdicts are unchanged. The live24h fixture, hub-only leaf fixture, donor and million-scale stores remain local. Approximately73GiB is free after retirement.
