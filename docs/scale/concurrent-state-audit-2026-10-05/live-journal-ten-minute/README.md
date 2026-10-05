# Experimental concurrent-state reader: live ten-minute journal campaign

Executed source `bc02e681402d5bfe5ec9a74b7de15f093fadf7ae`, normal profile,
seed1, five R5 servers, explicit route seeds, GOMAXPROCS2 / GOMEMLIMIT2GiB,
2m sync interval. Native named test PASS648.01s: 97 batches, 2,716 completed
workflows, 29,930 journal entries and 19 confirmed journal-leader SIGKILLs.
All six terminal/progress p99 cells are below30s; the largest terminal cell is
12.398054035s. Nine completed-cohort checkpoints pass, plus the mandatory final
whole-state audit and physical drain. Original20s/60s/three-attempt audit limits
remain unchanged; concurrent reader is explicitly selected and remains experimental.

The independent reviewer verifies every original archive member against its
manifest and closed original file, selected source bytes against executed Git,
SDK executable identity against the live process observation, observed server bytes
against the retained native server, and a fresh execution of the row checker.
SDK564524 SHA256 `29fc9caf655759acaf120ac422ef1b10d46f310514f086d2035d7cd0adfe464d`.
Server observation began during the campaign and captures14 process incarnations
across five logical nodes. It does not establish exhaustive lifetime coverage.
No original stores reopened and no unchanged workload rerun.

The initial independent review rejected an incorrect assumption of exactly five
container IDs. Restarts create replacement container IDs. Its exact failed script
and log are preserved; the corrected reviewer verifies all five logical node names,
all observed executable hashes and process closure.

This is one ten-minute changed-reader profile. It does not qualify the original
failed24h attempt, a full fault matrix, final-source qualification or the reader
as a new default, and does not demonstrate meaningful large-population speedup.
Complete producer originals, logs, retained source, executable and raw stores are
inside the split proof archive; verify/reconstruct using archive-verification.json.
