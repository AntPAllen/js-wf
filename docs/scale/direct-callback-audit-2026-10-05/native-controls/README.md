# Direct callback full-audit native race controls

Executed8b9b48b5a86a24c77876180a59bc4cd506fd8ded, actual SDK SHA256
`ebd1e82d8087ac1f31554d205a9d042e296f9af6677ad71a5546331b2dd83919`.
R3 in-process v2.15.0 server library linked into the observed race SDK.
Four suites PASS: compaction/cohort/fresh corruption69.74s, journal corruption
14.07s, state values14.68s, payload refill/holes/cutoff/cancellation15.03s.
Race timings are not speed evidence.

Full and captured-cohort reports and invariant errors match the independent
point reader. Direct callback is included alongside existing readers in the
oracle comparisons.116 retained256KiB records (30408704 payload bytes) match
sequence/header/data/time digest after deleting1/7/119 with cutoff119. Direct
and existing callback cancellation visits exactly one record; final consumer
count0. Payload crosses8MiB refill multiple times; shared leader gap oracle and
cutoff logic remain exercised.

Selected680 Git Go/module inputs, actual SDK race/VCS/module/bytes and closure
independently reviewed. No separate external server byte claim for linked R3
servers; source capture does not exhaust external compiler/toolchain inputs.
Full2163-member archive read back, one23009432-byte part, SHA256
`36f4aa54c6224705d5f419185e7be72985ea10422ca9d22735131e63a13c4f2f`.

No production direct-callback adoption, R1 replay/recovery, full400k capacity,
legacy compatibility or24h qualification. Native consumer-leader/cancel fault
controls are prepared separately, followed by legacy and capacity qualification.
