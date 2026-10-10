# Failed-child graph continuation development

The new native matrix covers R1/R3-domain, cached/buffered failed-child outcomes, and live/archive histories. Each parent catches the exact child error, carries the promise across two SDK checkpoints, finishes with 43, executes two parent effects and one child call, and rejects duplicate terminal appends. Archive cases collect original parent receipts and child terminal receipts before the final parent resume. Public continuation admission and production collection remain closed.

## Preserved attempts

- `command.json`, `initial-fixture.go.txt`, `race.log`: initial full eight-case race run, actual exit 1, 297.686 seconds. Six cases pass. R3 archive cached loses its lease; R3 archive buffered exhausts the inherited one-minute fixture budget. This direct-delivery helper had no heartbeat, unlike `RunPartition`; that omission is corrected. It does not independently prove every server-side cause.
- `heartbeat-command.json`, `heartbeat-fixture.go.txt`, `heartbeat-race.log`: the two R3 archive cases after adding the production renewal interval/freshness guard; actual exit 1, both expire at the aggregate fixture deadline.
- `phase-command.json`, `phase-fixture.go.txt`, `phase-race.log`: range-based parent audit plus phase timing, actual exit 1, 121.387 seconds. The cached case completes the second parent delivery at 52.505 seconds, then expires in collection. The buffered case reaches the final boundary scan at 59.173 seconds, then expires while collecting the retired child. No one-minute latency qualification is claimed.
- `boundary-command.json`, `boundary-fixture.go.txt`, `boundary-race.log`: source-hashed attempt combining final parent/child collection and using ordered traversal in production full-history/delivery reads. Actual result is retained in the command record.
- `range-journal-race.log`: the initial production traversal regression run, actual exit 1, 52.932 seconds. Three empty-history controls reject traversal because their record-only view intentionally publishes no pin. The fix preserves the validated metadata-only empty path.

The new failed-child archive scenario needs an explicit functional watchdog for provisioning, multiple deliveries, collection and audits. The separate strict 30-second worker/all-server kill-to-exact-ACK gate is unchanged. Full-current normal/race/extended simulation and all original remaining runtime/fault/scale/release requirements remain separate.

## Ordered full-history traversal

`GraphStore.Read`/`ReadExisting` and worker delivery suffix loading now use ordered range traversal. Existing entry, sequence/epoch, child-provenance and pin checks remain; full reads renew the same snapshot pin during traversal. The empty record-only path still returns the quorum-validated cursor without publishing a pin.

The focused journal race group passes in 52.405 seconds, actual exit 0, including 16 archive/collection scenarios, corrupt/unknown/stale/lease/generation/lifecycle controls and the previously failing three empty-history cases. A 33-record simulated-clock control reads identical complete history with **97 object GETs instead of 226**, while 19.4 simulated seconds elapse under a four-second pin TTL. It therefore requires repeated live-pin renewal. Restoring the prior full-read implementation through a read-only Go overlay makes that same census control fail at 226/226 GETs, actual exit 1. No recovery latency conclusion follows from this model census.

Reclamation probes now require `jetstream.ErrObjectNotFound`; unknown or timed-out reads cannot stand in for confirmed physical deletion. The final complete native failed-child race matrix is running with a two-minute watchdog only for its new failed-child archive cases; existing successful-child/archive watchdogs are unchanged. Actual final status and source-hashed retained race binaries are recorded separately.
