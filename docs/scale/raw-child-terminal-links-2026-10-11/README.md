# Retained child terminal projection linkage — 2026-10-11

The independent raw graph auditor now compares an annotated consumed child's
owned outcome body with the same-generation child's retained terminal projection.
The bytes must match exactly, as required by production child transfer. This
covers inline results, failed outcomes and external result descriptors. Both
owned external result bytes and owned signal body bytes retain their existing
independent hash/grant checks. A matching generation with a nonterminal cursor
is rejected. Projection reads must agree with the captured complete census.

`ChildTerminalLinksVerified` counts exact matching retained witnesses;
`ChildTerminalLinksUnavailable` counts absent roots, different generations and
missing projections. ID reuse cannot prove or disprove an old generation's
outcome. Retired projections supply retained bytes, not proof that the original
lost journal or original lease/publication history was correct.

## Regression controls

Baseline3549a11 plus the retained physical fixture accepted parent-consumed42
against retained child result43. The regression command exited1; the baseline
executable was not captured. Exact source commit, fixture, command and log are
retained. Physical reference auditing passes before each semantic check.

JSON/protobuf ×11 controls =22: ten positive and12 negative. Inline, failed and
external outcomes match; missing projection and reused-generation cases explicitly
assert unavailable witness counts. Mismatched inline result, error text,
external ref/hash, terminal kind and exact bytes are rejected. Retired child roots
are written through the real GraphStore on the in-memory transport; there is no
source or retained child journal after retirement. This deliberately tests the
available projection witness, not original child terminal ownership.

## Final verification

See command.json, native-command.json, executable.json, native-executable.json,
race.log, native.log and review.json for actual exits, timings and executable
hash/build/command captures. Source-inputs.json was captured before both drivers;
all captured inputs remained unchanged through completion.

The broad race command includes every raw graph top-level test and four native
checkpoint/census cases. The native command runs R1/R3 child and buffered-child
flows with explicit terminal/start witness assertions and raw checkpoint auditing.
Native stores are ephemeral; no storage restoration or power qualification is
claimed. The workflow adds every new physical control; review.py executes its
exact new guard against race.log and checks all four native witness receipts.
Hosted CI is not claimed.

Run `python3 docs/scale/raw-child-terminal-links-2026-10-11/review.py` from the
repository root to verify receipts, source hashes and guards.

Full original publication/parent-source/lease causality, unavailable old child
generations, retired original journal ownership, client linearizability,
current complete161/894 qualification and original scale/fault/soak/power gates
remain open. Four frozen long campaigns predate this change.
