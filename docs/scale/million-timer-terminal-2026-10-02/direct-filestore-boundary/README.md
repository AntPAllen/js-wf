# Direct file-store scheduling-index boundary

The reusable [runner](../../../../scripts/check-nats-scheduling-store-boundary.py)
and [fixture](../../../../scripts/fixtures/nats-scheduling-store-boundary_test.go.txt)
open fresh copies of the failed million-timer WF_RUN stores through unchanged
NATS 2.15.0 file-store and scheduler code. All six actual Go cases pass.

| Original replica | Physical messages in both cases | Intact index: recovered schedules | Removed copied index: recovered schedules |
| --- | ---: | ---: | ---: |
| node 0 | 768 | 0 | 768 |
| node 1 | 141 | 0 | 141 |
| node 2 | 0 | 0 | 0 |

Every case retains last sequence 2,000,000. Every rebuilt schedule points to a
readable source with a target header. Recovery mode keeps scheduling paused:
there are no server processes, Raft, clients, workflow handlers, publish callbacks
or target publications. This reproduces the recovery boundary directly in the
file store rather than requiring a full cluster restart.

All 4,998 original campaign files have matching before/after SHA256 hashes.
A complete source-copy inventory matches the original upstream module; the added
fixture and runner hashes match the committed files. The upstream module's own
Go dependencies are used (including nats.go 1.51.0); this is a file-store diagnostic,
not a claim to rebuild the original campaign binary's entire dependency graph.

The 21 top-level evidence files are archived losslessly with SHA256 readback.
Physical store copies and upstream build sources remain local and are excluded
from the published archive. Originals are in
`/tmp/js-wf-million-direct-filestore-actual-20261002`.

Two setup attempts were rejected before file-store execution: Go forbids module
cache overlays, and copied module directories initially retained read-only
permissions. Their originals remain in `/tmp/js-wf-million-direct-filestore-20261002`
and `/tmp/js-wf-million-direct-filestore-qualified-20261002`. The runner now uses
a separate exact module source copy and permits adding the fixture only there.

This isolates recovery suppression by the fully stamped empty scheduling index.
The initial persistence inconsistency cause remains unconfirmed. No original
index was repaired; the million-timer release verdict remains failed. Earlier
full-server copied-index rebuilding published duplicate targets, so this result
does not justify index removal on original stores.
