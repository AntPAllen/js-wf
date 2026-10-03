# Accepted six-component sustained mutation gate

The committed component checker exits zero against exact reference
`06762b6` (full revision in `qualification.json` inside the archive).
It independently rechecks all six raw ten-minute baseline/mutant pairs,
their successful job identities, original checkout logs, source inventories,
single-edit overlays, admission/audits, fault chronology, nanosecond latency
regeneration, semantic challenges and negative controls.

Five components execute at `c4fed061bc614488d4f89b53b216b756490f7da0`;
the corrected Start component executes at
`649c6ce8d40c10521da1a4f0feda066db4dc0332`. All 606 tracked Go/fixture/module
files, workflow bytes and each selected mutation definition are identical to
the reference. Differences in unrelated producer definitions do not substitute
for checking the selected executed mutation. Raw originals are retained in
[each component archive](../../current-sustained-mutations-2026-10-03/).

The twelve phases total 32,032 terminal invocations, 352,979 journal entries and
228 leader kills. Worst per-workflow terminal/progress p99 are
17.361650029/7.305135807 seconds. The report sets
`clears_sustained_six_mutation_gate=true`, `qualifies_parent_campaigns=false`
and `clears_full_release=false`. The failed original parent campaign remains
rejected. Full matrix/200-seed, final-source full100k and independent 24h
qualification remain separate.

The nine-member archive retains the exact executed input manifest, three
verifier source files, command/exit record, CLI logs, full qualification ledger
and summary. Every member was reopened and SHA-verified before atomic rename.
Manifest paths record the actual external review locations; to reproduce after
VM cleanup, extract each linked component's originals and remap those paths to
the extracted metadata, job logs and artifact directories. The verdict records
SHA256 for every original artifact file, metadata and log. Physical stores and
actual test executables were not uploaded or independently reopened.
