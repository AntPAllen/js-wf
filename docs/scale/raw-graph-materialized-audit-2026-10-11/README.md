# Independent materialized checkpoint consistency — 2026-10-11

Raw checkpoint auditing now validates state-key names and JSON values, promise
outcome object shape and reference/hash exclusivity, consumed signal ordering,
pending signal identity/order/cursor bounds and disjointness, cancelled timer
positions/order, and panic bounds. At the actual checkpoint completion it also
requires the persisted panic count to equal the raw journal prefix's attempt
count and the signal cursor to cover the prefix's consumed-signal high-water.
This uses an independent validator; production frame.validate/Decode are not
called. Shared wire types and unambiguous JSON decoding remain explicit.

## Verified focused scope

A retained race binary was compiled with `go test -race -c -o` and executed with
the exact selectors/options in binary.json. Actual child exit0, observed wall
21.486734s; executable SHA256
170a49d4d909cab92e4ff22b6f177e0b8be8744697e8bbb77da866e070791ded.
The receipt captures `-race=true`, the actual child PID, compilation/execution
commands and timestamps. Input hashes were captured before build, checked after
execution, and independently rechecked alongside the binary and raw log.
This remains development-worktree evidence, not complete clean-source testing.

Two richer JSON/protobuf frames pass. Nineteen physical-reference-valid semantic
controls reject state/promise names, nonobject/unknown outcome fields, hash
without reference, invalid pointer hash, simultaneous inline/pointer result,
zero/duplicate/unsorted consumed signals, zero/beyond-cursor/already-consumed/
invalid-name/duplicate pending signals, odd/beyond-position/duplicate cancelled
timers and a panic count inconsistent with the prefix.

All four native R1/R3 × ordinary/indexed layouts pass unpublished, published,
archived and terminal audits (sixteen receipts). These fixtures now contain
persisted state, an inline promise outcome and a cancelled timer; production
frame encoding/publication/archival constructs the native state. Existing raw
reference, journal and checkpoint-binding controls also pass. The CI guard was
executed successfully against race.log and requires every new control and both
positive encodings. Latest observed graph-publication hosted run38098486311 at
27fdb33 is queued, not accepted.

## Remaining limits

These are structural/value-set consistency checks. The pure rich fixture is not
a complete SDK-generated history. Reconstructing each materialized value and
consumed set from SDK history, validating the full signal queue/index, resolving
transitive promise result ownership, latest checkpoint discovery, protected
reader semantic replay, orphan projections and full scale/persistence acceptance
remain open. Neither phase completion nor admission/online collection is enabled.
The frozen00eeb13 complete159 normal and source6408a5 actual100000-entry normal
remain separate live campaigns; these additions do not restart or qualify them.
