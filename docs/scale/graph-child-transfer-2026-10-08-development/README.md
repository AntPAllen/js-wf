# Child-result transfer development artifacts

These are uncommitted development attempts, not frozen qualification or release evidence. Build errors and the rejected `SIM_SEEDS=100` configuration are retained in their raw logs. Failed runtime traces are preserved without rewriting their outcomes.

- Seed9 first used an async journal budget of5, causing a durable failure before the intended result-transfer boundary. The async path also records an AwaitSignal request: the budget is now6, and a new assertion requires initial suspension.
- Seed9 with budget6 durably records the intended journal-limit failure and NAKs that delivery. The test incorrectly required immediate ACK. It now checks the NAK and the replacement worker's terminal repair/ACK, then probes again after child retirement.
- Seed15 restored a deliberately forged signal's body but retained its old input-hash header. Production signal validation correctly rejected the mismatch. The fixture restores both bytes and hash.
- Seed121 retired the source while pinned, then advanced virtual time2s while the parent's upload intent TTL was1s. The source reader preserved its payload, but the parent correctly rejected publication after its own intent expired. The source-reader cut now advances500ms, within that unchanged parent TTL. This is a fixture timing correction, not a relaxed production publication condition.

All four have confirmed fixture causes; none is attributed to NATS. The final native controls cover R1/R3, sync/async and external results/external signal staging. The final modeled source terminal is an explicit small fixture; parent workflow, dispatch, graph transfer, replay and collection execute production code. Prepared child dispatch remains pending in the model, so graph-object drain does not claim whole legacy queue drain. Final frozen verification is recorded separately after committing code and pins.
