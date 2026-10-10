# Initial fixture failed — retained, not acceptance

Frozen94b9750, race package terminal exit1. All four native cases fail:
live cases compare raw payload bytes against the intentionally indented CLI JSON;
archive cases mistakenly test relocated archive receipts as if they were the
original physical entry receipts. Corrections normalize complete journal record
JSON for export comparison (owned object bytes remain exact) and census original
physical objects using logical hashes/current owned receipt references, matching
the established native archive matrix. No runtime implementation or gate changed.
The original binary, CLI/plugin and native stores remain under
`/home/exedev/js-wf-offline-replay-20261010` with hashes in state.json.
