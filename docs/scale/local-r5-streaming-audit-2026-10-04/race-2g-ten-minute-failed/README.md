# Failed combined-audit five-container race row

Executed source: `d7e075d8a1d2fce8b1b5c32e5a9f2d370213f83e`.
Actual race SDK: `8447a34b56a4ac64a08454f48f1a7882c74ee9483faa5242eedf68abd7bf3107`.

The requested ten-minute journal row failed after 346.89 seconds. Eight faults
healed; the ninth actual SIGKILL/restart of node3 failed the unchanged 60-second
replica-current heal deadline, with WF_JRN replica lag9058. The last four
checkpoint audits passed, maximum 2.658804 seconds. Last full checkpoint:
1120 invocations /12346 entries /1120 terminals. Audit 20s/60s bounds and2m sync
remain unchanged. This is not an audit timeout or a complete row pass.

The source inventory (1259 files), actual live SDK, every Go build-info field,
actual environment, post-run source/binary identity, raw fault operations and
four checkpoints independently verify. All4976 original members /189964740 bytes
read back against the producer ledger. Three original archive parts total
58282422 bytes; all part/concatenated hashes verify. Reviewer proof contains
three files,4477 bytes. See manifest.json, independent-review.json and the full
original-member-manifest.json. Concatenate numbered originals parts to recover
actual binary, source, raw histories, all broker files and15 failure snapshots.
All SDK/supervisor processes are terminal and fixture containers removed.

Route snapshots show node3 has four pooled connections, all to node0; other
nodes have12–16 connections. See route-adjacency-review.json with original member
hashes. This identifies a topology observation, not the cause of replication
failure or proof of a fix. Original stores remain retained, not reopened. No
final full-state/drain/model/complete-row/full-matrix/24h qualification is claimed.
