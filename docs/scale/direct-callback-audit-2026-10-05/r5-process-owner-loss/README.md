# Actual R5 process-owner loss: original failure preserved

Executed clean69f2dc7, race SDK, current actualv2.15.0 external servers,
four CPUs/GOGC500/4GiB. Parent FAIL334.46s; neither fixture qualifies.

- Owner left down: complete1500/6000 audit in4.040516067s, pending4039;
  post-audit `js.Stream(ctx, name)` receives no reply and expires at the outer
  five-minute fixture context. Independent standard stream-info requests on
  allfour surviving servers report zero INV/JRN consumers. Missing reply cause
  unconfirmed; no retained absence/server-defect claim.
- Same-store owner restart: at visit1961 rejects replay sequence1 with
  `retained batch scan: unexpected stream/order WF_JRN/1`; elapsed3.563312441s.
  This records incomplete reduction, not missing stored journals. Existing strict
  R1 replay admission remains unchanged. Actual owner PID replacement verified.

Independent681 selected Git Go/module inputs, actual SDK/race identity,
11 observed external server processes/bytes/module/mounts and all process closure
verified. Complete1777-member archive67,834,754bytes, three parts, SHA256
`d720c735b654d383d8f1bc87324cbdee06e9ed1d285f96402129c90ccef72c15`, all members
and parts read back. Stores remain closed and must never be reopened.

Next changed diagnostic bounds cleanup requests within original20s, retaining
all observations, and records actual cursor API positions/creation/identity at
replay proof. No production replay/retry/default change or fault capacity/24h claim.
