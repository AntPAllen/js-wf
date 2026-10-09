# Explicit cursor selection and owned Await retry — 2026-10-09

The operator and worker commands now share `-graph-cursor-version 4|5|6` selection. Version 4 remains the existing default. Version 5 selects the checkpoint index; version 6 selects archive checkpoints for new isolated stores. All three store names are required, invalid versions fail before connecting, and existing cursor schemas are rejected rather than upgraded. Worker timer/retention restrictions remain enforced.

Native operator tests cover six v4/v5/v6 × R1/R3-domain cases: eight complete logical records, describe/export equality, zero legacy journal requests, zero wrong domain requests, no surviving readers, and two incorrect-schema rejections with unchanged authority. Version 6 compacts and collects the original entry receipt before inspection. Ordinary worker tests cover the same six version/replication cases: reserved Start/Signal recovery, native timer completion, two deleted projection restorations with unchanged journal, no legacy journal writes and acknowledged repair events. Default v4 cases omit the new flag. These worker tests do not execute staged continuation handlers.

## Await behavior and model controls

Owned terminal reads retry a typed CAS conflict from a fresh view of the originally captured invocation. Each terminal read attempt has a five-second local deadline; only actual expiration accompanied by a deadline error permits retry. Caller cancellation, immediate unknown deadline responses and corruption continue to fail closed. Corruption returned at the local deadline is explicitly tested. Reader-release uncertainty cannot return successful result bytes. Cleanup retains its separate three-second bound.

Fourteen directed v4/v6 modes × 16 seeds (224 bodies and exact replays) cover root/read/release contention, source replacement, matching purge markers, cancellation and immediate unknown errors. Two real timer controls exercise local deadline retry and corruption at expiry. These controls are component fixtures, not a new registered shared Tier-1 family or saved regression corpus. The complete frozen 149-family acceptance excludes these changes.

## Failures retained

- `await-old-code-failure.log` and twelve saved traces reproduce definite contention returning errors before the retry change; the two unknown-error controls already passed.
- `initial-export-formatting-failure.log` records the test comparing compact payload bytes with CLI-indented JSON. The test now compares compacted JSON while requiring all logical records.
- `initial-worker-contention-failure.log` records a native v6 R3 Await reader-release conflict.
- `initial-unbounded-read-cli-race-failure.log` records v5 R3 reaching its original two-minute caller deadline.
- `final-normal.log` records v5 R1 cancelling before terminal-repair acknowledgement was observed. The fixture now waits for acknowledgement inside its unchanged deadline before graceful shutdown.
- `final-race.log` records another v5 R3 two-minute timeout even with local terminal-read deadlines. A focused retest passed in 93.401 seconds (`diagnostic-v5-race.log`); this does not establish the timeout's cause or eliminate the broader native liveness risk. Live diagnostics showed repeated five-second uncertain Start binding operations followed by successful recovery. No scan deadline or native acceptance target was increased.

`commands.json`, source observations and raw logs bind the component runs. `executed-review.py` requires unchanged repository Go/module inputs, all required native/directed cases, normal/race terminal package success, actual process exit zero and no race/failure events. A verdict is generated only for a fully passing terminal campaign. The acknowledgement-synchronized normal campaign passed; its race campaign terminated with exit 1: all v4/v5/v6 R3 worker cases missed the two-minute deadline (worker package 448.401 s). All R1 worker cases and the client/operator race packages passed. The native worker race gate is not accepted. Retained failure events again show pending Start binding repeatedly exhausting the five-second scan; the native liveness cause remains open. The operator runtime workflow now wires separate client/operator/worker race jobs; hosted execution remains unqualified until observed.

Shared Tier-1 integration of Await contention, complete current/race/extended qualification, continuation CLI execution/public admission, import/deployment migration, native autonomous collection and every original fault/scale/soak/drain/release requirement remain open. Production collection and public continuation admission remain disabled.
