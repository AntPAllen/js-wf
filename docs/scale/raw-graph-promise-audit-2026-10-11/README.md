# Independent checkpoint promise result ownership — 2026-10-11

Each external promise outcome in the current checkpoint frame must have its
result hash on an owned payload edge of that checkpoint completion. The audit
loads that physical object with the configured byte bound and checks its hash.
Logical references cannot alias different hashes, including collision with the
frame's own reference. Mentioning a result somewhere in an earlier prefix does
not replace current-checkpoint ownership. Production readers/materialization
helpers are not called; the preceding raw reference walk independently checks
the same edges' grants and origins.

Two JSON/protobuf positive fixtures pass. Three reference-valid controls reject
an omitted result edge, an owned edge containing different bytes, and a logical
reference reused for conflicting hashes. All four native R1/R3 × ordinary/indexed
layouts now include an external promise result at the checkpoint completion and
pass unpublished/published/archived/terminal audits (sixteen receipts). This
native fixture uses production publication APIs; it is not an SDK child history.
Existing reference/journal/frame controls also pass.

The retained race binary exits0, observed19.983813s; SHA256
315e435e9e546755ae6ee56c798600e05958b2dc95f6fa43514a7ea6c4041847.
run.py preserves compilation/execution, source hashes, actual child PID/exits,
binary build info and raw logs. Independent review rechecked inputs/binary/log
and executed the CI coverage guard against that log. This is focused development
race evidence. Full SDK history/provenance reconstruction, checkpoint metadata,
protected-reader semantics and original scale/fault/soak acceptance remain open.
